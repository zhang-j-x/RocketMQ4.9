/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.client.impl.consumer;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.consumer.store.LocalFileOffsetStore;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.consumer.store.RemoteBrokerOffsetStore;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;
import org.apache.rocketmq.client.hook.FilterMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.filter.FilterAPI;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.*;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.body.ConsumeStatus;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.body.ProcessQueueInfo;
import org.apache.rocketmq.common.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

public class DefaultMQPushConsumerImpl implements MQConsumerInner {
    /**
     * Delay some time when exception occur
     */
    private long pullTimeDelayMillsWhenException = 3000;
    /**
     * Flow control interval
     */
    private static final long PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL = 50;
    /**
     * Delay some time when suspend pull service
     */
    private static final long PULL_TIME_DELAY_MILLS_WHEN_SUSPEND = 1000;
    private static final long BROKER_SUSPEND_MAX_TIME_MILLIS = 1000 * 15;
    private static final long CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND = 1000 * 30;
    private final InternalLogger log = ClientLogger.getLog();
    /**消费者门面 管理配置信息*/
    private final DefaultMQPushConsumer defaultMQPushConsumer;
    /**负载均衡实现对象 分配订阅主题的队列给消费者 20秒一个周期执行负载均衡 由客户端实例触发*/
    private final RebalanceImpl rebalanceImpl = new RebalancePushImpl(this);
    /**过滤消息的Hook*/
    private final ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<FilterMessageHook>();
    /**消费者启动时间*/
    private final long consumerStartTimestamp = System.currentTimeMillis();
    /**消息处理前后Hook 分别在消息处理前和处理后执行hook.before和hook.after*/
    private final ArrayList<ConsumeMessageHook> consumeMessageHookList = new ArrayList<ConsumeMessageHook>();
    /**request类型请求在业务处理器处理之前会调用RPCHook的doBeforeRequest方法，在业务处理器之后会调用doAfterResponse方法*/
    private final RPCHook rpcHook;
    /**消费者状态*/
    private volatile ServiceState serviceState = ServiceState.CREATE_JUST;
    /**客户端实例*/
    private MQClientInstance mQClientFactory;
    /**拉消息api封装 内部有推荐下次pull选择broker的算法*/
    private PullAPIWrapper pullAPIWrapper;
    /**是否暂停*/
    private volatile boolean pause = false;
    /**是否顺序消费*/
    private boolean consumeOrderly = false;
    /**消息监听器*/
    private MessageListener messageListenerInner;
    /**消费进度存储*/
    private OffsetStore offsetStore;
    /**消费消息服务*/
    private ConsumeMessageService consumeMessageService;
    /**流控次数 1000次流控打印一次日志*/
    private long queueFlowControlTimes = 0;
    /**并发消费 消息跨度流控次数*/
    private long queueMaxSpanFlowControlTimes = 0;

    public DefaultMQPushConsumerImpl(DefaultMQPushConsumer defaultMQPushConsumer, RPCHook rpcHook) {
        this.defaultMQPushConsumer = defaultMQPushConsumer;
        this.rpcHook = rpcHook;
        this.pullTimeDelayMillsWhenException = defaultMQPushConsumer.getPullTimeDelayMillsWhenException();
    }

    public void registerFilterMessageHook(final FilterMessageHook hook) {
        this.filterMessageHookList.add(hook);
        log.info("register FilterMessageHook Hook, {}", hook.hookName());
    }

    public boolean hasHook() {
        return !this.consumeMessageHookList.isEmpty();
    }

    public void registerConsumeMessageHook(final ConsumeMessageHook hook) {
        this.consumeMessageHookList.add(hook);
        log.info("register consumeMessageHook Hook, {}", hook.hookName());
    }

    public void executeHookBefore(final ConsumeMessageContext context) {
        if (!this.consumeMessageHookList.isEmpty()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageBefore(context);
                } catch (Throwable e) {
                }
            }
        }
    }

    public void executeHookAfter(final ConsumeMessageContext context) {
        if (!this.consumeMessageHookList.isEmpty()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageAfter(context);
                } catch (Throwable e) {
                }
            }
        }
    }

    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, newTopic, queueNum, 0);
    }

    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
        this.mQClientFactory.getMQAdminImpl().createTopic(key, newTopic, queueNum, topicSysFlag);
    }

    public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        Set<MessageQueue> result = this.rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
        if (null == result) {
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            result = this.rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
        }

        if (null == result) {
            throw new MQClientException("The topic[" + topic + "] not exist", null);
        }

        return parseSubscribeMessageQueues(result);
    }

    public Set<MessageQueue> parseSubscribeMessageQueues(Set<MessageQueue> messageQueueList) {
        Set<MessageQueue> resultQueues = new HashSet<MessageQueue>();
        for (MessageQueue queue : messageQueueList) {
            String userTopic = NamespaceUtil.withoutNamespace(queue.getTopic(), this.defaultMQPushConsumer.getNamespace());
            resultQueues.add(new MessageQueue(userTopic, queue.getBrokerName(), queue.getQueueId()));
        }

        return resultQueues;
    }

    public DefaultMQPushConsumer getDefaultMQPushConsumer() {
        return defaultMQPushConsumer;
    }

    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().earliestMsgStoreTime(mq);
    }

    public long maxOffset(MessageQueue mq) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
    }

    public long minOffset(MessageQueue mq) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().minOffset(mq);
    }

    public OffsetStore getOffsetStore() {
        return offsetStore;
    }

    public void setOffsetStore(OffsetStore offsetStore) {
        this.offsetStore = offsetStore;
    }

    /**
     * 拉消息请求
     * @param pullRequest
     */
    public void pullMessage(final PullRequest pullRequest) {
        final ProcessQueue processQueue = pullRequest.getProcessQueue();
        //如果某个处理队列为删除状态，可说明重平衡后，该topic队列被分配给其他消费者了
        if (processQueue.isDropped()) {
            log.info("the pull request[{}] is dropped.", pullRequest.toString());
            return;
        }

        pullRequest.getProcessQueue().setLastPullTimestamp(System.currentTimeMillis());

        try {
            //检查消费者状态 如果消费者不是RUNNING状态则延迟3秒后再执行
            this.makeSureStateOK();
        } catch (MQClientException e) {
            log.warn("pullMessage exception, consumer state not ok", e);
            this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
            return;
        }

        //如果消费者处于暂停状态则延迟1秒后再执行
        if (this.isPause()) {
            log.warn("consumer was paused, execute pull request later. instanceName={}, group={}", this.defaultMQPushConsumer.getInstanceName(), this.defaultMQPushConsumer.getConsumerGroup());
            this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_SUSPEND);
            return;
        }
        //缓存消息数量
        long cachedMessageCount = processQueue.getMsgCount().get();
        //缓存消息大小
        long cachedMessageSizeInMiB = processQueue.getMsgSize().get() / (1024 * 1024);

        //如果缓存消息数量 大于 队列流控配置pullThresholdForQueue的数量大小（默认1000）,即本地未消费的消息大于1000条, 则延迟50毫秒拉取
        if (cachedMessageCount > this.defaultMQPushConsumer.getPullThresholdForQueue()) {
            this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
            if ((queueFlowControlTimes++ % 1000) == 0) {
                log.warn(
                    "the cached message count exceeds the threshold {}, so do flow control, minOffset={}, maxOffset={}, count={}, size={} MiB, pullRequest={}, flowControlTimes={}",
                    this.defaultMQPushConsumer.getPullThresholdForQueue(), processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), cachedMessageCount, cachedMessageSizeInMiB, pullRequest, queueFlowControlTimes);
            }
            return;
        }

        //如果缓存消息大小 大于 队列流控配置pullThresholdSizeForQueue的数量大小（默认100MD），即本地未消费的消息大小大于100MB, 则延迟50毫秒拉取
        if (cachedMessageSizeInMiB > this.defaultMQPushConsumer.getPullThresholdSizeForQueue()) {
            this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
            if ((queueFlowControlTimes++ % 1000) == 0) {
                log.warn(
                    "the cached message size exceeds the threshold {} MiB, so do flow control, minOffset={}, maxOffset={}, count={}, size={} MiB, pullRequest={}, flowControlTimes={}",
                    this.defaultMQPushConsumer.getPullThresholdSizeForQueue(), processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), cachedMessageCount, cachedMessageSizeInMiB, pullRequest, queueFlowControlTimes);
            }
            return;
        }


        if (!this.consumeOrderly) {
            //并发消费
            /**
             * 快照队列最后一条消息和第一条消息的offset差值 大于并发消息消费时处理队列的最大跨度配置consumeConcurrentlyMaxSpan（默认2000）， 则延迟50毫秒拉取
             * 快照队列最后一条消息和第一条消息的offset差值 大于2000并不能说明有2000条消息，存在客户端和服务端过滤消息的逻辑
             */
            if (processQueue.getMaxSpan() > this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan()) {
                this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
                if ((queueMaxSpanFlowControlTimes++ % 1000) == 0) {
                    log.warn(
                        "the queue's messages, span too long, so do flow control, minOffset={}, maxOffset={}, maxSpan={}, pullRequest={}, flowControlTimes={}",
                        processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), processQueue.getMaxSpan(),
                        pullRequest, queueMaxSpanFlowControlTimes);
                }
                return;
            }
        } else {
            //顺序消费
            if (processQueue.isLocked()) {
                //如果消费队列 broker端分布式锁由当前消费者持有
                //判断是否是当前消费者第一次拉取该消费队列消息（重平衡后该消费队列作为新队列分配给了当前消费者）
                if (!pullRequest.isPreviouslyLocked()) {
                    long offset = -1L;
                    try {
                        //如果是新分配给当前消费者的消费队列，且第一次拉取消息，从broker端获取该消费队列的消费进度
                        offset = this.rebalanceImpl.computePullFromWhereWithException(pullRequest.getMessageQueue());
                    } catch (MQClientException e) {
                        this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
                        log.error("Failed to compute pull offset, pullResult: {}", pullRequest, e);
                        return;
                    }

                    boolean brokerBusy = offset < pullRequest.getNextOffset();
                    log.info("the first time to pull message, so fix offset from broker. pullRequest: {} NewOffset: {} brokerBusy: {}",
                        pullRequest, offset, brokerBusy);
                    if (brokerBusy) {
                        //如果拉取进度大于broker端存储的消费进度  打印相关日志
                        log.info("[NOTIFYME]the first time to pull message, but pull request offset larger than broker consume offset. pullRequest: {} NewOffset: {}",
                            pullRequest, offset);
                    }
                    //锁定 表明已经拉取过一次消息 下次拉取消息时，不需要校验拉取偏移量是否比broker端消费进度大了
                    pullRequest.setPreviouslyLocked(true);
                    //设置拉消息偏移量为broker端的消费进度
                    pullRequest.setNextOffset(offset);
                }
            } else {
                //如果当前消费者并没有持有消费队列在broker端的分布式锁 则创建一个延迟任务 3s后重新将消息拉取请求加入到PullMessageService的请求队列
                this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
                log.info("pull message later because not locked in broker, {}", pullRequest);
                return;
            }
        }

        //获取本次拉消息请求topic的订阅数据
        final SubscriptionData subscriptionData = this.rebalanceImpl.getSubscriptionInner().get(pullRequest.getMessageQueue().getTopic());
        /**
         * subscriptionData== null时 3秒后重试
         * 什么时候null == subscriptionData？
         *  消费者调用unsubscribe 取消订阅主题
         *  重平衡中最后一步 遍历processQueueTable，如果订阅信息表subscriptionInner中没有该主题，则将该队列从processQueueTable中移除，并设置processQueue为删除状态
         *  所以下次重平衡后对应的processQueue的状态会设置为删除
         */
        if (null == subscriptionData) {
            this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
            log.warn("find the consumer's subscription failed, {}", pullRequest);
            return;
        }

        final long beginTimestamp = System.currentTimeMillis();

        //拉消息处理回调
        PullCallback pullCallback = new PullCallback() {
            @Override
            public void onSuccess(PullResult pullResult) {
                if (pullResult != null) {
                    //预处理拉消息结果
                    pullResult = DefaultMQPushConsumerImpl.this.pullAPIWrapper.processPullResult(pullRequest.getMessageQueue(), pullResult,
                        subscriptionData);

                    switch (pullResult.getPullStatus()) {
                        case FOUND:
                            //正常情况 从服务器拉取到消息

                            //发起拉消息请求的消息偏移量
                            long prevRequestOffset = pullRequest.getNextOffset();
                            //更新拉取请求的下次拉取消息的偏移量
                            pullRequest.setNextOffset(pullResult.getNextBeginOffset());

                            //监控相关
                            long pullRT = System.currentTimeMillis() - beginTimestamp;
                            DefaultMQPushConsumerImpl.this.getConsumerStatsManager().incPullRT(pullRequest.getConsumerGroup(),
                                pullRequest.getMessageQueue().getTopic(), pullRT);

                            long firstMsgOffset = Long.MAX_VALUE;
                            /**
                             * 什么时候为空？
                             *  客户端过滤被过滤掉了
                             */
                            if (pullResult.getMsgFoundList() == null || pullResult.getMsgFoundList().isEmpty()) {
                                //立马发起下一次消息拉取
                                DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                            } else {
                                //获取本次拉取消息的第一条消息的偏移量
                                firstMsgOffset = pullResult.getMsgFoundList().get(0).getQueueOffset();

                                //监控相关
                                DefaultMQPushConsumerImpl.this.getConsumerStatsManager().incPullTPS(pullRequest.getConsumerGroup(),
                                    pullRequest.getMessageQueue().getTopic(), pullResult.getMsgFoundList().size());

                                /**
                                 * 将所有消息放入processQueue
                                 *
                                 */
                                boolean dispatchToConsume = processQueue.putMessage(pullResult.getMsgFoundList());

                                /**
                                 * 提交消费任务
                                 * @param msgs 从服务器端拉取的消息
                                 * @param processQueue 本地消息处理队列
                                 * @param messageQueue  topic
                                 * @param dispatchToConsume 该参数只有顺序消费服务才有效
                                 */
                                DefaultMQPushConsumerImpl.this.consumeMessageService.submitConsumeRequest(
                                    pullResult.getMsgFoundList(),
                                    processQueue,
                                    pullRequest.getMessageQueue(),
                                    dispatchToConsume);

                                //判断配置消息拉取有没有间隔 默认为0
                                if (DefaultMQPushConsumerImpl.this.defaultMQPushConsumer.getPullInterval() > 0) {
                                    DefaultMQPushConsumerImpl.this.executePullRequestLater(pullRequest,
                                        DefaultMQPushConsumerImpl.this.defaultMQPushConsumer.getPullInterval());
                                } else {
                                    //将更新过下次拉取偏移量的消息拉取请求重新放入消息拉取服务PullMessageService的请求队列pullRequestQueue中
                                    DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                                }
                            }

                            //如果返回的下次拉消息请求的偏移量比请求的偏移量小 或者 第一条消息在consumequeue的偏移量小于请求的偏移量 打警告日志
                            if (pullResult.getNextBeginOffset() < prevRequestOffset
                                || firstMsgOffset < prevRequestOffset) {
                                log.warn(
                                    "[BUG] pull message result maybe data wrong, nextBeginOffset: {} firstMsgOffset: {} prevRequestOffset: {}",
                                    pullResult.getNextBeginOffset(),
                                    firstMsgOffset,
                                    prevRequestOffset);
                            }

                            break;
                        case NO_NEW_MSG:
                        case NO_MATCHED_MSG:
                            //没有拉取到可消费的新消息

                            //更新下次拉取请求的偏移量
                            pullRequest.setNextOffset(pullResult.getNextBeginOffset());
                            //更新topic最新的offset
                            DefaultMQPushConsumerImpl.this.correctTagsOffset(pullRequest);
                            //将更新过下次拉取偏移量的消息拉取请求重新放入消息拉取服务PullMessageService的请求队列pullRequestQueue中
                            DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                            break;
                        case OFFSET_ILLEGAL:
                            /**
                             * 消息拉取使用的offset非法
                             *  1、offset < minOffset
                             *  2、offset > maxOffset
                             *  3、队列没有消息  pullResult.getNextBeginOffset() 为0
                             */

                            log.warn("the pull request offset illegal, {} {}",
                                pullRequest.toString(), pullResult.toString());
                            //调整为服务端返回的正确的offset
                            pullRequest.setNextOffset(pullResult.getNextBeginOffset());
                            //设置topic队列对应的消息处理队列ProcessQueue设置为删除状态，对应的topic队列的消费任务会中止
                            pullRequest.getProcessQueue().setDropped(true);
                            //提交一个延时任务 10s后执行
                            DefaultMQPushConsumerImpl.this.executeTaskLater(new Runnable() {

                                @Override
                                public void run() {
                                    try {
                                        //更新offsetStore中topic队列的offset为正确值，increaseOnly为false，内部直接替换，不做比较操作
                                        DefaultMQPushConsumerImpl.this.offsetStore.updateOffset(pullRequest.getMessageQueue(),
                                            pullRequest.getNextOffset(), false);

                                        //持久化该topic队列的offset到broker端
                                        DefaultMQPushConsumerImpl.this.offsetStore.persist(pullRequest.getMessageQueue());
                                        //删除消费组topic队列对应处理队列
                                        DefaultMQPushConsumerImpl.this.rebalanceImpl.removeProcessQueue(pullRequest.getMessageQueue());

                                        /**
                                         * 注意 这里并没有提交pullRequest到PullMessageService的请求队列pullRequestQueue中，但是重平衡服务执行重平衡时
                                         * 遍历执行重平衡策略后分给给当前消费者的队列，如果队列不在processQueueTable中，则又会为topic队列重建ProcessQueue，
                                         * 保存到processQueueTable，并且创建新的拉消息请求提交给PullMessageService，放入消息拉取服务PullMessageService的请求队列pullRequestQueue中
                                         */
                                        log.warn("fix the pull request offset, {}", pullRequest);
                                    } catch (Throwable e) {
                                        log.error("executeTaskLater Exception", e);
                                    }
                                }
                            }, 10000);
                            break;
                        default:
                            break;
                    }
                }
            }

            @Override
            public void onException(Throwable e) {
                if (!pullRequest.getMessageQueue().getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    log.warn("execute the pull request exception", e);
                }

                DefaultMQPushConsumerImpl.this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
            }
        };

        //是否提交消费者本地的offset
        boolean commitOffsetEnable = false;
        //该队列本地的offset
        long commitOffsetValue = 0L;
        if (MessageModel.CLUSTERING == this.defaultMQPushConsumer.getMessageModel()) {
            //从offsetStore中获取offset
            commitOffsetValue = this.offsetStore.readOffset(pullRequest.getMessageQueue(), ReadOffsetType.READ_FROM_MEMORY);
            if (commitOffsetValue > 0) {
                commitOffsetEnable = true;
            }
        }

        //过滤表达式
        String subExpression = null;
        //是否类过滤模式
        boolean classFilter = false;
        //获取主题的订阅数据
        SubscriptionData sd = this.rebalanceImpl.getSubscriptionInner().get(pullRequest.getMessageQueue().getTopic());
        if (sd != null) {
            if (this.defaultMQPushConsumer.isPostSubscriptionWhenPull() && !sd.isClassFilterMode()) {
                subExpression = sd.getSubString();
            }

            classFilter = sd.isClassFilterMode();
        }

        /**
         * 高4位未使用
         * 低4位：
         *      第1位：是否提交消费者本地该队列的offset （一般是1）
         *      第2位：是否允许服务器端长轮询 （一般是1）
         *      第3位：是否提交消费者本地该主题的订阅信息 （一般是0）
         *      第4位：是否为类过滤 （一般是0）
         */
        int sysFlag = PullSysFlag.buildSysFlag(
            commitOffsetEnable, // commitOffset
            true, // suspend
            subExpression != null, // subscription
            classFilter // class filter
        );
        try {

            /**
             * @param mq 拉消息队列
             * @param subExpression 客户端topic订阅数据中的过滤表达式
             * @param expressionType 表达式类型
             * @param subVersion 客户端topic订阅数据的版本
             * @param offset 拉消息的nextOffset
             * @param maxNums 拉消息的数量
             * @param sysFlag 拉取系统标记
             *          * 高4位未使用
             *          * 低4位：
             *          *      第1位：是否提交消费者本地该队列的offset （一般是1）
             *          *      第2位：是否允许服务器端长轮询 （一般是1）
             *          *      第3位：是否提交消费者本地该主题的订阅信息 （一般是0）
             *          *      第4位：是否为类过滤 （一般是0）
             * @param commitOffset 本地该队列的消费进度
             * @param brokerSuspendMaxTimeMillis 服务器长轮询时 连接最长保持时间 15s
             * @param timeoutMillis  RPC超时时间 30s
             * @param communicationMode RPC调用模式 异步
             * @param pullCallback 拉消息结果回调
             */
            this.pullAPIWrapper.pullKernelImpl(
                pullRequest.getMessageQueue(),
                subExpression,
                subscriptionData.getExpressionType(),
                subscriptionData.getSubVersion(),
                pullRequest.getNextOffset(),
                this.defaultMQPushConsumer.getPullBatchSize(),
                sysFlag,
                commitOffsetValue,
                BROKER_SUSPEND_MAX_TIME_MILLIS,
                CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND,
                CommunicationMode.ASYNC,
                pullCallback
            );
        } catch (Exception e) {
            log.error("pullKernelImpl exception", e);
            this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
        }
    }

    private void makeSureStateOK() throws MQClientException {
        if (this.serviceState != ServiceState.RUNNING) {
            throw new MQClientException("The consumer service state not OK, "
                + this.serviceState
                + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                null);
        }
    }

    private void executePullRequestLater(final PullRequest pullRequest, final long timeDelay) {
        this.mQClientFactory.getPullMessageService().executePullRequestLater(pullRequest, timeDelay);
    }

    public boolean isPause() {
        return pause;
    }

    public void setPause(boolean pause) {
        this.pause = pause;
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return this.mQClientFactory.getConsumerStatsManager();
    }

    public void executePullRequestImmediately(final PullRequest pullRequest) {
        this.mQClientFactory.getPullMessageService().executePullRequestImmediately(pullRequest);
    }

    private void correctTagsOffset(final PullRequest pullRequest) {
        if (0L == pullRequest.getProcessQueue().getMsgCount().get()) {
            this.offsetStore.updateOffset(pullRequest.getMessageQueue(), pullRequest.getNextOffset(), true);
        }
    }

    public void executeTaskLater(final Runnable r, final long timeDelay) {
        this.mQClientFactory.getPullMessageService().executeTaskLater(r, timeDelay);
    }

    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
        throws MQClientException, InterruptedException {
        return this.mQClientFactory.getMQAdminImpl().queryMessage(topic, key, maxNum, begin, end);
    }

    public MessageExt queryMessageByUniqKey(String topic, String uniqKey) throws MQClientException,
        InterruptedException {
        return this.mQClientFactory.getMQAdminImpl().queryMessageByUniqKey(topic, uniqKey);
    }

    public void registerMessageListener(MessageListener messageListener) {
        this.messageListenerInner = messageListener;
    }

    public void resume() {
        this.pause = false;
        doRebalance();
        log.info("resume this consumer, {}", this.defaultMQPushConsumer.getConsumerGroup());
    }

    /**
     * 将消息回退到broker
     * @param msg 消息
     * @param delayLevel 延迟级别
     * @param brokerName broker名称
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    public void sendMessageBack(MessageExt msg, int delayLevel, final String brokerName)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        try {
            String brokerAddr = (null != brokerName) ? this.mQClientFactory.findBrokerAddressInPublish(brokerName)
                : RemotingHelper.parseSocketAddressAddr(msg.getStoreHost());
            //消息回退 这个是发到主题对应的重试主题
            this.mQClientFactory.getMQClientAPIImpl().consumerSendMessageBack(brokerAddr, msg,
                this.defaultMQPushConsumer.getConsumerGroup(), delayLevel, 5000, getMaxReconsumeTimes());
        } catch (Exception e) {
            //如果回退失败了 就重发一条消息
            log.error("sendMessageBack Exception, " + this.defaultMQPushConsumer.getConsumerGroup(), e);

            Message newMsg = new Message(MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup()), msg.getBody());

            String originMsgId = MessageAccessor.getOriginMessageId(msg);
            MessageAccessor.setOriginMessageId(newMsg, UtilAll.isBlank(originMsgId) ? msg.getMsgId() : originMsgId);

            newMsg.setFlag(msg.getFlag());
            MessageAccessor.setProperties(newMsg, msg.getProperties());
            //设置重试主题属性
            MessageAccessor.putProperty(newMsg, MessageConst.PROPERTY_RETRY_TOPIC, msg.getTopic());
            //重复消费次数
            MessageAccessor.setReconsumeTime(newMsg, String.valueOf(msg.getReconsumeTimes() + 1));
            //最大重复消费次数
            MessageAccessor.setMaxReconsumeTimes(newMsg, String.valueOf(getMaxReconsumeTimes()));
            MessageAccessor.clearProperty(newMsg, MessageConst.PROPERTY_TRANSACTION_PREPARED);
            newMsg.setDelayTimeLevel(3 + msg.getReconsumeTimes());

            this.mQClientFactory.getDefaultMQProducer().send(newMsg);
        } finally {
            msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQPushConsumer.getNamespace()));
        }
    }

    private int getMaxReconsumeTimes() {
        // default reconsume times: 16
        if (this.defaultMQPushConsumer.getMaxReconsumeTimes() == -1) {
            return 16;
        } else {
            return this.defaultMQPushConsumer.getMaxReconsumeTimes();
        }
    }

    public void shutdown() {
        shutdown(0);
    }

    public synchronized void shutdown(long awaitTerminateMillis) {
        switch (this.serviceState) {
            case CREATE_JUST:
                break;
            case RUNNING:
                this.consumeMessageService.shutdown(awaitTerminateMillis);
                this.persistConsumerOffset();
                this.mQClientFactory.unregisterConsumer(this.defaultMQPushConsumer.getConsumerGroup());
                this.mQClientFactory.shutdown();
                log.info("the consumer [{}] shutdown OK", this.defaultMQPushConsumer.getConsumerGroup());
                this.rebalanceImpl.destroy();
                this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                break;
            case SHUTDOWN_ALREADY:
                break;
            default:
                break;
        }
    }

    public synchronized void start() throws MQClientException {
        switch (this.serviceState) {
            case CREATE_JUST:
                log.info("the consumer [{}] start beginning. messageModel={}, isUnitMode={}", this.defaultMQPushConsumer.getConsumerGroup(),
                    this.defaultMQPushConsumer.getMessageModel(), this.defaultMQPushConsumer.isUnitMode());
                //1、将消费者启动状态设置为启动失败状态ServiceState.START_FAILED，最开始创建时为创建状态 ServiceState.CREATE_JUST 启动成功后立马改为成功
                this.serviceState = ServiceState.START_FAILED;
                //2、检查消费者配置 如消费者组不为空，消费模式不为空，topic队列分配策略不为空等
                this.checkConfig();
                /**
                 * 3、拷贝订阅信息
                 * （1）获取门面对象的订阅信息 遍历封装为SubscriptionData 加入重平衡对象实现对象RebalanceImpl的订阅信息表中
                 * （2）集群模式下订阅当前消费者组的重试主题： “%RETRY%” + 消费组组名，将当前消费者组的重试主题订阅数据放入重平衡实现对象RebalanceImpl的订阅信息表中
                 */
                this.copySubscription();

                if (this.defaultMQPushConsumer.getMessageModel() == MessageModel.CLUSTERING) {
                    //4、修改消费者实例名称，如果是默认名则改为PID
                    this.defaultMQPushConsumer.changeInstanceNameToPID();
                }
                //5、获取客户端实例，如果客户端实例已经初始化，返回初始化对象
                this.mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(this.defaultMQPushConsumer, this.rpcHook);

                /**
                 * 6、设置重平衡对象消费组 、消费模式 、队列分配策略、客户端实例属性
                 */
                this.rebalanceImpl.setConsumerGroup(this.defaultMQPushConsumer.getConsumerGroup());
                this.rebalanceImpl.setMessageModel(this.defaultMQPushConsumer.getMessageModel());
                this.rebalanceImpl.setAllocateMessageQueueStrategy(this.defaultMQPushConsumer.getAllocateMessageQueueStrategy());
                this.rebalanceImpl.setmQClientFactory(this.mQClientFactory);

                //7、创建拉消息API对象（内部封装了查询主机推荐算法）
                this.pullAPIWrapper = new PullAPIWrapper(
                    mQClientFactory,
                    this.defaultMQPushConsumer.getConsumerGroup(), isUnitMode());
                //将过滤hook列表注册到pullApi内，消息拉取下拉后会执行该hook，再进行一次自定义的过滤
                this.pullAPIWrapper.registerFilterMessageHook(filterMessageHookList);


                //8、初始化消息消费进度管理器，并设置为defaultMQPushConsumer的属性值
                if (this.defaultMQPushConsumer.getOffsetStore() != null) {
                    this.offsetStore = this.defaultMQPushConsumer.getOffsetStore();
                } else {
                    switch (this.defaultMQPushConsumer.getMessageModel()) {
                        case BROADCASTING:
                            //广播模式 消息消费进度存储在客户端
                            this.offsetStore = new LocalFileOffsetStore(this.mQClientFactory, this.defaultMQPushConsumer.getConsumerGroup());
                            break;
                        case CLUSTERING:
                            //集群模式 消息消费进度存储在broker
                            this.offsetStore = new RemoteBrokerOffsetStore(this.mQClientFactory, this.defaultMQPushConsumer.getConsumerGroup());
                            break;
                        default:
                            break;
                    }
                    //将创建的store对象设置到门面对象
                    this.defaultMQPushConsumer.setOffsetStore(this.offsetStore);
                }
                //集群模式下 什么都没做
                this.offsetStore.load();


                //9、根据是否顺序消费，创建消费端消费线程服务，并启动
                if (this.getMessageListenerInner() instanceof MessageListenerOrderly) {
                    this.consumeOrderly = true;
                    //顺序消费服务
                    this.consumeMessageService =
                        new ConsumeMessageOrderlyService(this, (MessageListenerOrderly) this.getMessageListenerInner());
                } else if (this.getMessageListenerInner() instanceof MessageListenerConcurrently) {
                    this.consumeOrderly = false;
                    //并发消费服务
                    this.consumeMessageService =
                        new ConsumeMessageConcurrentlyService(this, (MessageListenerConcurrently) this.getMessageListenerInner());
                }

                //启动消费服务
                this.consumeMessageService.start();

                /**
                 * 10、把消费者注册到客户端实例中
                 * 客户端实例给消费者提供了什么服务？
                 *      1、心跳服务（把订阅数据同步到关注主题的broker）
                 *      2、拉消息服务（内部PullMessageService启动线程，基于PullRequestQueue工作，消费者负载均衡分配到队列后，会向该队列提交pullRequest）
                 *      3、队列负载服务（每20秒调用一次consumer.doRebalance()接口）
                 *      4、消费进度持久化
                 *      5、动态调整消费者消费服务线程池（已经注释掉了）
                 */
                boolean registerOK = mQClientFactory.registerConsumer(this.defaultMQPushConsumer.getConsumerGroup(), this);
                if (!registerOK) {
                    this.serviceState = ServiceState.CREATE_JUST;
                    this.consumeMessageService.shutdown(defaultMQPushConsumer.getAwaitTerminationMillisWhenShutdown());
                    throw new MQClientException("The consumer group[" + this.defaultMQPushConsumer.getConsumerGroup()
                        + "] has been created before, specify another name please." + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL),
                        null);
                }

                //11.启动客户端实例
                mQClientFactory.start();
                log.info("the consumer [{}] start OK.", this.defaultMQPushConsumer.getConsumerGroup());
                //设置消费者状态为ServiceState.RUNNING
                this.serviceState = ServiceState.RUNNING;
                break;
            case RUNNING:
            case START_FAILED:
            case SHUTDOWN_ALREADY:
                throw new MQClientException("The PushConsumer service state not OK, maybe started once, "
                    + this.serviceState
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                    null);
            default:
                break;
        }

        //从NameServer更新该消费者关注主题的路由信息数据   最后把主题路由数据放到重平衡实现对象的订阅主题队列信息表中
        this.updateTopicSubscribeInfoWhenSubscriptionChanged();
        //检查broker是否支持消费者的消息过滤模式（一般都是tag过滤 broker默认支持）
        this.mQClientFactory.checkClientInBroker();
        //向所有的已知broker发送心跳
        this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
        //唤醒重平衡线程 立即负载均衡
        this.mQClientFactory.rebalanceImmediately();
    }

    private void checkConfig() throws MQClientException {
        Validators.checkGroup(this.defaultMQPushConsumer.getConsumerGroup());

        //消费者组不能为空
        if (null == this.defaultMQPushConsumer.getConsumerGroup()) {
            throw new MQClientException(
                "consumerGroup is null"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        //消费者组不能为默认值
        if (this.defaultMQPushConsumer.getConsumerGroup().equals(MixAll.DEFAULT_CONSUMER_GROUP)) {
            throw new MQClientException(
                "consumerGroup can not equal "
                    + MixAll.DEFAULT_CONSUMER_GROUP
                    + ", please specify another one."
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        //消费模式不能为空
        if (null == this.defaultMQPushConsumer.getMessageModel()) {
            throw new MQClientException(
                "messageModel is null"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        //consumeFromWhere不能为空
        if (null == this.defaultMQPushConsumer.getConsumeFromWhere()) {
            throw new MQClientException(
                "consumeFromWhere is null"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }
        //消费者启动时间不能为空
        Date dt = UtilAll.parseDate(this.defaultMQPushConsumer.getConsumeTimestamp(), UtilAll.YYYYMMDDHHMMSS);
        if (null == dt) {
            throw new MQClientException(
                "consumeTimestamp is invalid, the valid format is yyyyMMddHHmmss,but received "
                    + this.defaultMQPushConsumer.getConsumeTimestamp()
                    + " " + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
        }

        // 集群模式下消息队列的负载策略不能为空
        if (null == this.defaultMQPushConsumer.getAllocateMessageQueueStrategy()) {
            throw new MQClientException(
                "allocateMessageQueueStrategy is null"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // 订阅不能为空
        if (null == this.defaultMQPushConsumer.getSubscription()) {
            throw new MQClientException(
                "subscription is null"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // 消息监听器不为空
        if (null == this.defaultMQPushConsumer.getMessageListener()) {
            throw new MQClientException(
                "messageListener is null"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        boolean orderly = this.defaultMQPushConsumer.getMessageListener() instanceof MessageListenerOrderly;
        boolean concurrently = this.defaultMQPushConsumer.getMessageListener() instanceof MessageListenerConcurrently;
        if (!orderly && !concurrently) {
            throw new MQClientException(
                "messageListener must be instanceof MessageListenerOrderly or MessageListenerConcurrently"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // 消费者线程数最小值 1至1000
        if (this.defaultMQPushConsumer.getConsumeThreadMin() < 1
            || this.defaultMQPushConsumer.getConsumeThreadMin() > 1000) {
            throw new MQClientException(
                "consumeThreadMin Out of range [1, 1000]"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // 消费者线程数最大值 1至1000
        if (this.defaultMQPushConsumer.getConsumeThreadMax() < 1 || this.defaultMQPushConsumer.getConsumeThreadMax() > 1000) {
            throw new MQClientException(
                "consumeThreadMax Out of range [1, 1000]"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        //  消费者线程数最大值 必须大于等于 消费者线程数最小值
        if (this.defaultMQPushConsumer.getConsumeThreadMin() > this.defaultMQPushConsumer.getConsumeThreadMax()) {
            throw new MQClientException(
                "consumeThreadMin (" + this.defaultMQPushConsumer.getConsumeThreadMin() + ") "
                    + "is larger than consumeThreadMax (" + this.defaultMQPushConsumer.getConsumeThreadMax() + ")",
                null);
        }

        // 并发消息消费时处理队列的最大跨度 1至65535
        if (this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan() < 1
            || this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan() > 65535) {
            throw new MQClientException(
                "consumeConcurrentlyMaxSpan Out of range [1, 65535]"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        //  队列流控消息数量限制 1至65535
        if (this.defaultMQPushConsumer.getPullThresholdForQueue() < 1 || this.defaultMQPushConsumer.getPullThresholdForQueue() > 65535) {
            throw new MQClientException(
                "pullThresholdForQueue Out of range [1, 65535]"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // 主题流控消息数量限制 1至6553500
        if (this.defaultMQPushConsumer.getPullThresholdForTopic() != -1) {
            if (this.defaultMQPushConsumer.getPullThresholdForTopic() < 1 || this.defaultMQPushConsumer.getPullThresholdForTopic() > 6553500) {
                throw new MQClientException(
                    "pullThresholdForTopic Out of range [1, 6553500]"
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
            }
        }

        // 队列流控消息大小限制  1MiB 至 1024 MiB
        if (this.defaultMQPushConsumer.getPullThresholdSizeForQueue() < 1 || this.defaultMQPushConsumer.getPullThresholdSizeForQueue() > 1024) {
            throw new MQClientException(
                "pullThresholdSizeForQueue Out of range [1, 1024]"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        //主题流控消息大小限制  1MiB 至 102400 MiB
        if (this.defaultMQPushConsumer.getPullThresholdSizeForTopic() != -1) {
            // pullThresholdSizeForTopic
            if (this.defaultMQPushConsumer.getPullThresholdSizeForTopic() < 1 || this.defaultMQPushConsumer.getPullThresholdSizeForTopic() > 102400) {
                throw new MQClientException(
                    "pullThresholdSizeForTopic Out of range [1, 102400]"
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
            }
        }

        // 推模式下 拉取任务的间隔时间 1至 65535
        if (this.defaultMQPushConsumer.getPullInterval() < 0 || this.defaultMQPushConsumer.getPullInterval() > 65535) {
            throw new MQClientException(
                "pullInterval Out of range [0, 65535]"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // 消费任务最多可消费消息的数量 1至1024
        if (this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize() < 1
            || this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize() > 1024) {
            throw new MQClientException(
                "consumeMessageBatchMaxSize Out of range [1, 1024]"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // 每次消息拉取拉取的消息数量 1至 1024
        if (this.defaultMQPushConsumer.getPullBatchSize() < 1 || this.defaultMQPushConsumer.getPullBatchSize() > 1024) {
            throw new MQClientException(
                "pullBatchSize Out of range [1, 1024]"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }
    }

    private void copySubscription() throws MQClientException {
        try {
            //获取门面对象的订阅信息 遍历封装为SubscriptionData 加入重平衡对象实现对象RebalanceImpl的订阅信息表中
            Map<String, String> sub = this.defaultMQPushConsumer.getSubscription();
            if (sub != null) {
                for (final Map.Entry<String, String> entry : sub.entrySet()) {
                    final String topic = entry.getKey();
                    final String subString = entry.getValue();
                    SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(topic, subString);
                    this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
                }
            }

            if (null == this.messageListenerInner) {
                //将门面对象的MessageListener保存到消费者实现对象
                this.messageListenerInner = this.defaultMQPushConsumer.getMessageListener();
            }

            switch (this.defaultMQPushConsumer.getMessageModel()) {
                case BROADCASTING:
                    break;
                case CLUSTERING:
                    /**
                     * 集群模式
                     * 为什么会订阅当前消费者组的重试主题： “%RETRY%” + 消费组组名
                     *  消息重试时 消息会最终加入到该主题 消费者订阅这个主题后，就有机会再次消费该消息
                     */
                    //获取当前消费者组的重试主题 “%RETRY%” + 消费组组名
                    final String retryTopic = MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup());
                    //创建“%RETRY%” + 消费组组名 主题的订阅数据对象
                    SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(retryTopic, SubscriptionData.SUB_ALL);
                    //将当前消费者组的重试主题订阅数据放入重平衡实现对象的订阅信息表中
                    this.rebalanceImpl.getSubscriptionInner().put(retryTopic, subscriptionData);
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    public MessageListener getMessageListenerInner() {
        return messageListenerInner;
    }

    private void updateTopicSubscribeInfoWhenSubscriptionChanged() {
        //获取消费者的订阅信息 key: topic value: 过滤信息
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            for (final Map.Entry<String, SubscriptionData> entry : subTable.entrySet()) {
                final String topic = entry.getKey();
                //以topic为维度从nameServe更新路由数据
                this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            }
        }
    }

    public ConcurrentMap<String, SubscriptionData> getSubscriptionInner() {
        return this.rebalanceImpl.getSubscriptionInner();
    }

    public void subscribe(String topic, String subExpression) throws MQClientException {
        try {
            //1、构建订阅数据
            SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(topic, subExpression);
            //2、将订阅数据放到rebalanceImpl的订阅信息表中
            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
            if (this.mQClientFactory != null) {
                //3、通过心跳将订阅信息发给broker
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
            }
        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    public void subscribe(String topic, String fullClassName, String filterClassSource) throws MQClientException {
        try {
            SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(topic, "*");
            subscriptionData.setSubString(fullClassName);
            subscriptionData.setClassFilterMode(true);
            subscriptionData.setFilterClassSource(filterClassSource);
            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
            if (this.mQClientFactory != null) {
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
            }

        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    public void subscribe(final String topic, final MessageSelector messageSelector) throws MQClientException {
        try {
            if (messageSelector == null) {
                subscribe(topic, SubscriptionData.SUB_ALL);
                return;
            }

            SubscriptionData subscriptionData = FilterAPI.build(topic,
                messageSelector.getExpression(), messageSelector.getExpressionType());

            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
            if (this.mQClientFactory != null) {
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
            }
        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    public void suspend() {
        this.pause = true;
        log.info("suspend this consumer, {}", this.defaultMQPushConsumer.getConsumerGroup());
    }

    public void unsubscribe(String topic) {
        this.rebalanceImpl.getSubscriptionInner().remove(topic);
    }

    public void updateConsumeOffset(MessageQueue mq, long offset) {
        this.offsetStore.updateOffset(mq, offset, false);
    }

    public void updateCorePoolSize(int corePoolSize) {
        this.consumeMessageService.updateCorePoolSize(corePoolSize);
    }

    public MessageExt viewMessage(String msgId)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return this.mQClientFactory.getMQAdminImpl().viewMessage(msgId);
    }

    public RebalanceImpl getRebalanceImpl() {
        return rebalanceImpl;
    }

    public boolean isConsumeOrderly() {
        return consumeOrderly;
    }

    public void setConsumeOrderly(boolean consumeOrderly) {
        this.consumeOrderly = consumeOrderly;
    }

    public void resetOffsetByTimeStamp(long timeStamp)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        for (String topic : rebalanceImpl.getSubscriptionInner().keySet()) {
            Set<MessageQueue> mqs = rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
            Map<MessageQueue, Long> offsetTable = new HashMap<MessageQueue, Long>();
            if (mqs != null) {
                for (MessageQueue mq : mqs) {
                    long offset = searchOffset(mq, timeStamp);
                    offsetTable.put(mq, offset);
                }
                this.mQClientFactory.resetOffset(topic, groupName(), offsetTable);
            }
        }
    }

    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
    }

    @Override
    public String groupName() {
        return this.defaultMQPushConsumer.getConsumerGroup();
    }

    @Override
    public MessageModel messageModel() {
        return this.defaultMQPushConsumer.getMessageModel();
    }

    @Override
    public ConsumeType consumeType() {
        return ConsumeType.CONSUME_PASSIVELY;
    }

    @Override
    public ConsumeFromWhere consumeFromWhere() {
        return this.defaultMQPushConsumer.getConsumeFromWhere();
    }

    @Override
    public Set<SubscriptionData> subscriptions() {
        Set<SubscriptionData> subSet = new HashSet<SubscriptionData>();

        subSet.addAll(this.rebalanceImpl.getSubscriptionInner().values());

        return subSet;
    }

    /**
     * RebalanceService服务每20s调用一次
     */
    @Override
    public void doRebalance() {
        //如果当前消费者没有暂停消费
        if (!this.pause) {
            // 做重平衡操作
            this.rebalanceImpl.doRebalance(this.isConsumeOrderly());
        }
    }

    @Override
    public void persistConsumerOffset() {
        try {
            this.makeSureStateOK();
            Set<MessageQueue> mqs = new HashSet<MessageQueue>();
            Set<MessageQueue> allocateMq = this.rebalanceImpl.getProcessQueueTable().keySet();
            mqs.addAll(allocateMq);

            this.offsetStore.persistAll(mqs);
        } catch (Exception e) {
            log.error("group: " + this.defaultMQPushConsumer.getConsumerGroup() + " persistConsumerOffset exception", e);
        }
    }

    @Override
    public void updateTopicSubscribeInfo(String topic, Set<MessageQueue> info) {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            if (subTable.containsKey(topic)) {
                this.rebalanceImpl.topicSubscribeInfoTable.put(topic, info);
            }
        }
    }

    @Override
    public boolean isSubscribeTopicNeedUpdate(String topic) {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            if (subTable.containsKey(topic)) {
                return !this.rebalanceImpl.topicSubscribeInfoTable.containsKey(topic);
            }
        }

        return false;
    }

    @Override
    public boolean isUnitMode() {
        return this.defaultMQPushConsumer.isUnitMode();
    }

    @Override
    public ConsumerRunningInfo consumerRunningInfo() {
        ConsumerRunningInfo info = new ConsumerRunningInfo();

        Properties prop = MixAll.object2Properties(this.defaultMQPushConsumer);

        prop.put(ConsumerRunningInfo.PROP_CONSUME_ORDERLY, String.valueOf(this.consumeOrderly));
        prop.put(ConsumerRunningInfo.PROP_THREADPOOL_CORE_SIZE, String.valueOf(this.consumeMessageService.getCorePoolSize()));
        prop.put(ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP, String.valueOf(this.consumerStartTimestamp));

        info.setProperties(prop);

        Set<SubscriptionData> subSet = this.subscriptions();
        info.getSubscriptionSet().addAll(subSet);

        Iterator<Entry<MessageQueue, ProcessQueue>> it = this.rebalanceImpl.getProcessQueueTable().entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            MessageQueue mq = next.getKey();
            ProcessQueue pq = next.getValue();

            ProcessQueueInfo pqinfo = new ProcessQueueInfo();
            pqinfo.setCommitOffset(this.offsetStore.readOffset(mq, ReadOffsetType.MEMORY_FIRST_THEN_STORE));
            pq.fillProcessQueueInfo(pqinfo);
            info.getMqTable().put(mq, pqinfo);
        }

        for (SubscriptionData sd : subSet) {
            ConsumeStatus consumeStatus = this.mQClientFactory.getConsumerStatsManager().consumeStatus(this.groupName(), sd.getTopic());
            info.getStatusTable().put(sd.getTopic(), consumeStatus);
        }

        return info;
    }

    public MQClientInstance getmQClientFactory() {
        return mQClientFactory;
    }

    public void setmQClientFactory(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }

    public ServiceState getServiceState() {
        return serviceState;
    }

    //Don't use this deprecated setter, which will be removed soon.
    @Deprecated
    public synchronized void setServiceState(ServiceState serviceState) {
        this.serviceState = serviceState;
    }

    public void adjustThreadPool() {
        long computeAccTotal = this.computeAccumulationTotal();
        long adjustThreadPoolNumsThreshold = this.defaultMQPushConsumer.getAdjustThreadPoolNumsThreshold();

        long incThreshold = (long) (adjustThreadPoolNumsThreshold * 1.0);

        long decThreshold = (long) (adjustThreadPoolNumsThreshold * 0.8);

        if (computeAccTotal >= incThreshold) {
            this.consumeMessageService.incCorePoolSize();
        }

        if (computeAccTotal < decThreshold) {
            this.consumeMessageService.decCorePoolSize();
        }
    }

    private long computeAccumulationTotal() {
        long msgAccTotal = 0;
        ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = this.rebalanceImpl.getProcessQueueTable();
        Iterator<Entry<MessageQueue, ProcessQueue>> it = processQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            ProcessQueue value = next.getValue();
            msgAccTotal += value.getMsgAccCnt();
        }

        return msgAccTotal;
    }

    public List<QueueTimeSpan> queryConsumeTimeSpan(final String topic)
        throws RemotingException, MQClientException, InterruptedException, MQBrokerException {
        List<QueueTimeSpan> queueTimeSpan = new ArrayList<QueueTimeSpan>();
        TopicRouteData routeData = this.mQClientFactory.getMQClientAPIImpl().getTopicRouteInfoFromNameServer(topic, 3000);
        for (BrokerData brokerData : routeData.getBrokerDatas()) {
            String addr = brokerData.selectBrokerAddr();
            queueTimeSpan.addAll(this.mQClientFactory.getMQClientAPIImpl().queryConsumeTimeSpan(addr, topic, groupName(), 3000));
        }

        return queueTimeSpan;
    }

    /**
     *
     * @param msgs 消息
     * @param consumerGroup 消费者组
     */
    public void resetRetryAndNamespace(final List<MessageExt> msgs, String consumerGroup) {
        //获取当前消费者组的重试主题 %RETRY% + 消费者组组名
        final String groupTopic = MixAll.getRetryTopic(consumerGroup);
        for (MessageExt msg : msgs) {
            //获取每条消息的重试主题 （一般消息没有该属性 只有重复消费的消息有该属性）
            String retryTopic = msg.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);

            //说明该消息时被重复消费的消息
            if (retryTopic != null && groupTopic.equals(msg.getTopic())) {
                //将重复消费的消息主题修改为原始主题
                msg.setTopic(retryTopic);
            }

            if (StringUtils.isNotEmpty(this.defaultMQPushConsumer.getNamespace())) {
                msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQPushConsumer.getNamespace()));
            }
        }
    }

    public ConsumeMessageService getConsumeMessageService() {
        return consumeMessageService;
    }

    public void setConsumeMessageService(ConsumeMessageService consumeMessageService) {
        this.consumeMessageService = consumeMessageService;

    }

    public void setPullTimeDelayMillsWhenException(long pullTimeDelayMillsWhenException) {
        this.pullTimeDelayMillsWhenException = pullTimeDelayMillsWhenException;
    }
}
