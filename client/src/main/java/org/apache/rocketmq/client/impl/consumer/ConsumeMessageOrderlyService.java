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
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeReturnType;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.*;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.body.CMResult;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;

/**
 * 顺序消费
 */
public class ConsumeMessageOrderlyService implements ConsumeMessageService {
    private static final InternalLogger log = ClientLogger.getLog();
    private final static long MAX_TIME_CONSUME_CONTINUOUSLY =
        Long.parseLong(System.getProperty("rocketmq.client.maxTimeConsumeContinuously", "60000"));
    /**消费者*/
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
    /**消费门面对象*/
    private final DefaultMQPushConsumer defaultMQPushConsumer;
    /**消息监听器 消息消费业务逻辑*/
    private final MessageListenerOrderly messageListener;
    /**消息消费任务队列*/
    private final BlockingQueue<Runnable> consumeRequestQueue;
    /**消息消费线程池*/
    private final ThreadPoolExecutor consumeExecutor;
    /**消息者组*/
    private final String consumerGroup;
    /**本地topic队列锁容器*/
    private final MessageQueueLock messageQueueLock = new MessageQueueLock();
    /**调度任务线程池（1、执行续约锁任务 2、延迟提交消费任务）*/
    private final ScheduledExecutorService scheduledExecutorService;
    private volatile boolean stopped = false;

    public ConsumeMessageOrderlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl,
        MessageListenerOrderly messageListener) {
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
        this.messageListener = messageListener;

        this.defaultMQPushConsumer = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer();
        this.consumerGroup = this.defaultMQPushConsumer.getConsumerGroup();
        this.consumeRequestQueue = new LinkedBlockingQueue<Runnable>();

        this.consumeExecutor = new ThreadPoolExecutor(
            this.defaultMQPushConsumer.getConsumeThreadMin(),
            this.defaultMQPushConsumer.getConsumeThreadMax(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.consumeRequestQueue,
            new ThreadFactoryImpl("ConsumeMessageThread_"));

        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumeMessageScheduledThread_"));
    }

    public void start() {
        if (MessageModel.CLUSTERING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())) {
            //默认每20s执行一次锁定分配给自己的消息消费队列
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    ConsumeMessageOrderlyService.this.lockMQPeriodically();
                }
            }, 1000 * 1, ProcessQueue.REBALANCE_LOCK_INTERVAL, TimeUnit.MILLISECONDS);
        }
    }

    public void shutdown(long awaitTerminateMillis) {
        this.stopped = true;
        this.scheduledExecutorService.shutdown();
        ThreadUtils.shutdownGracefully(this.consumeExecutor, awaitTerminateMillis, TimeUnit.MILLISECONDS);
        if (MessageModel.CLUSTERING.equals(this.defaultMQPushConsumerImpl.messageModel())) {
            this.unlockAllMQ();
        }
    }

    public synchronized void unlockAllMQ() {
        this.defaultMQPushConsumerImpl.getRebalanceImpl().unlockAll(false);
    }

    @Override
    public void updateCorePoolSize(int corePoolSize) {
        if (corePoolSize > 0
            && corePoolSize <= Short.MAX_VALUE
            && corePoolSize < this.defaultMQPushConsumer.getConsumeThreadMax()) {
            this.consumeExecutor.setCorePoolSize(corePoolSize);
        }
    }

    @Override
    public void incCorePoolSize() {
    }

    @Override
    public void decCorePoolSize() {
    }

    @Override
    public int getCorePoolSize() {
        return this.consumeExecutor.getCorePoolSize();
    }

    @Override
    public ConsumeMessageDirectlyResult consumeMessageDirectly(MessageExt msg, String brokerName) {
        ConsumeMessageDirectlyResult result = new ConsumeMessageDirectlyResult();
        result.setOrder(true);

        List<MessageExt> msgs = new ArrayList<MessageExt>();
        msgs.add(msg);
        MessageQueue mq = new MessageQueue();
        mq.setBrokerName(brokerName);
        mq.setTopic(msg.getTopic());
        mq.setQueueId(msg.getQueueId());

        ConsumeOrderlyContext context = new ConsumeOrderlyContext(mq);

        this.defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, this.consumerGroup);

        final long beginTime = System.currentTimeMillis();

        log.info("consumeMessageDirectly receive new message: {}", msg);

        try {
            ConsumeOrderlyStatus status = this.messageListener.consumeMessage(msgs, context);
            if (status != null) {
                switch (status) {
                    case COMMIT:
                        result.setConsumeResult(CMResult.CR_COMMIT);
                        break;
                    case ROLLBACK:
                        result.setConsumeResult(CMResult.CR_ROLLBACK);
                        break;
                    case SUCCESS:
                        result.setConsumeResult(CMResult.CR_SUCCESS);
                        break;
                    case SUSPEND_CURRENT_QUEUE_A_MOMENT:
                        result.setConsumeResult(CMResult.CR_LATER);
                        break;
                    default:
                        break;
                }
            } else {
                result.setConsumeResult(CMResult.CR_RETURN_NULL);
            }
        } catch (Throwable e) {
            result.setConsumeResult(CMResult.CR_THROW_EXCEPTION);
            result.setRemark(RemotingHelper.exceptionSimpleDesc(e));

            log.warn(String.format("consumeMessageDirectly exception: %s Group: %s Msgs: %s MQ: %s",
                RemotingHelper.exceptionSimpleDesc(e),
                ConsumeMessageOrderlyService.this.consumerGroup,
                msgs,
                mq), e);
        }

        result.setAutoCommit(context.isAutoCommit());
        result.setSpentTimeMills(System.currentTimeMillis() - beginTime);

        log.info("consumeMessageDirectly Result: {}", result);

        return result;
    }

    /**
     * 提交消费任务
     * @param msgs 消息
     * @param processQueue topic队列对应消息处理队列
     * @param messageQueue topic队列
     * @param dispathToConsume true 创建消费任务并提交
     *                         false  不创建消费任务  说明消费者本地已经有消费任务在执行了
     */
    @Override
    public void submitConsumeRequest(
        final List<MessageExt> msgs,
        final ProcessQueue processQueue,
        final MessageQueue messageQueue,
        final boolean dispathToConsume) {
        if (dispathToConsume) {
            ConsumeRequest consumeRequest = new ConsumeRequest(processQueue, messageQueue);
            this.consumeExecutor.submit(consumeRequest);
        }
    }

    /**
     * 消费者向broker发起请求锁定分配给自己的消费队列
     */
    public synchronized void lockMQPeriodically() {
        if (!this.stopped) {
            this.defaultMQPushConsumerImpl.getRebalanceImpl().lockAll();
        }
    }

    /**
     * 尝试锁定然后重消费
     *      延迟指定时间后 尝试获取队列对应的broker端分布式锁
     *          如果获取成功 10ms后提交消费任务 否则 3秒后提交消费任务
     * @param mq 需要获取broker端分布式锁的消费队列
     * @param processQueue 消费队列对应处理队列
     * @param delayMills 延迟时间
     */
    public void tryLockLaterAndReconsume(final MessageQueue mq, final ProcessQueue processQueue,
        final long delayMills) {
        this.scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                boolean lockOK = ConsumeMessageOrderlyService.this.lockOneMQ(mq);
                if (lockOK) {
                    ConsumeMessageOrderlyService.this.submitConsumeRequestLater(processQueue, mq, 10);
                } else {
                    ConsumeMessageOrderlyService.this.submitConsumeRequestLater(processQueue, mq, 3000);
                }
            }
        }, delayMills, TimeUnit.MILLISECONDS);
    }

    public synchronized boolean lockOneMQ(final MessageQueue mq) {
        if (!this.stopped) {
            return this.defaultMQPushConsumerImpl.getRebalanceImpl().lock(mq);
        }

        return false;
    }

    private void submitConsumeRequestLater(
        final ProcessQueue processQueue,
        final MessageQueue messageQueue,
        final long suspendTimeMillis
    ) {
        long timeMillis = suspendTimeMillis;
        if (timeMillis == -1) {
            timeMillis = this.defaultMQPushConsumer.getSuspendCurrentQueueTimeMillis();
        }

        if (timeMillis < 10) {
            timeMillis = 10;
        } else if (timeMillis > 30000) {
            timeMillis = 30000;
        }

        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                ConsumeMessageOrderlyService.this.submitConsumeRequest(null, processQueue, messageQueue, true);
            }
        }, timeMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * 顺序消费消费结果处理
     * @param msgs 本轮从从ProcessQueue获取的消息集合（默认1条）
     * @param status 消费状态
     * @param context 消费上下文
     * @param consumeRequest 消费任务
     * @return 是否继续循环处理ProcessQueue内的消息
     *              返回false时顺序消费任务退出
     *              返回true时 继续循环从ProcessQueue中获取消息 进行消费
     */
    public boolean processConsumeResult(
        final List<MessageExt> msgs,
        final ConsumeOrderlyStatus status,
        final ConsumeOrderlyContext context,
        final ConsumeRequest consumeRequest
    ) {
        boolean continueConsume = true;
        long commitOffset = -1L;
        //是否自动提交
        if (context.isAutoCommit()) {
            switch (status) {
                case COMMIT:
                case ROLLBACK:
                    log.warn("the message queue consume result is illegal, we think you want to ack these message {}",
                        consumeRequest.getMessageQueue());
                case SUCCESS:

                    //将本次循环处理的消息 从临时map中删除，并且返回消费进度
                    commitOffset = consumeRequest.getProcessQueue().commit();
                    this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());
                    break;
                case SUSPEND_CURRENT_QUEUE_A_MOMENT:
                    this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());
                    /**
                     * 判断是否还有重试次数
                     *      1、重试未超过最大重试次数 返回true 表示可以重试
                     *      2、重试超过最大重试次数 先尝试回退消息到broker
                     *              如果回退成功 返回false 不再重试
                     *              如果回退失败 则返回true 继续重试
                     * @param msgs
                     * @return true 表示可以重试  false 不再重试
                     */
                    if (checkReconsumeTimes(msgs)) {
                        //本地重试 把消息从consumingMsgOrderlyTreeMap移除 重新加入msgTreeMap
                        consumeRequest.getProcessQueue().makeMessageToConsumeAgain(msgs);
                        //1s后提交顺序消费任务
                        this.submitConsumeRequestLater(
                            consumeRequest.getProcessQueue(),
                            consumeRequest.getMessageQueue(),
                            context.getSuspendCurrentQueueTimeMillis());
                        continueConsume = false;
                    } else {
                        //如果没有本地重试次数了 直接提交消费进度，提交消费进度的时候会清空consumingMsgOrderlyTreeMap
                        commitOffset = consumeRequest.getProcessQueue().commit();
                    }
                    break;
                default:
                    break;
            }
        } else {
            switch (status) {
                case SUCCESS:
                    this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());
                    break;
                case COMMIT:
                    commitOffset = consumeRequest.getProcessQueue().commit();
                    break;
                case ROLLBACK:
                    consumeRequest.getProcessQueue().rollback();
                    this.submitConsumeRequestLater(
                        consumeRequest.getProcessQueue(),
                        consumeRequest.getMessageQueue(),
                        context.getSuspendCurrentQueueTimeMillis());
                    continueConsume = false;
                    break;
                case SUSPEND_CURRENT_QUEUE_A_MOMENT:
                    this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());

                    /**
                     * 判断是否还有重试次数
                     *      1、重试未超过最大重试次数 返回true 表示可以重试
                     *      2、重试超过最大重试次数 先尝试回退消息
                     *              如果回退成功 返回false 不再重试
                     *              如果回退失败 则返回true 继续重试
                     */
                    if (checkReconsumeTimes(msgs)) {

                        //将消息重新放回ProcessQueue的msgTreeMap中
                        consumeRequest.getProcessQueue().makeMessageToConsumeAgain(msgs);

                        //再次提交消费任务 默认延迟1s
                        this.submitConsumeRequestLater(
                            consumeRequest.getProcessQueue(),
                            consumeRequest.getMessageQueue(),
                            context.getSuspendCurrentQueueTimeMillis());
                        //外层会退出本次消费任务
                        continueConsume = false;
                    }
                    break;
                default:
                    break;
            }
        }

        if (commitOffset >= 0 && !consumeRequest.getProcessQueue().isDropped()) {
            //更新本地消费进度 对应topic队列的消费进度为commitOffset
            this.defaultMQPushConsumerImpl.getOffsetStore().updateOffset(consumeRequest.getMessageQueue(), commitOffset, false);
        }

        return continueConsume;
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return this.defaultMQPushConsumerImpl.getConsumerStatsManager();
    }

    private int getMaxReconsumeTimes() {
        // default reconsume times: Integer.MAX_VALUE
        if (this.defaultMQPushConsumer.getMaxReconsumeTimes() == -1) {
            return Integer.MAX_VALUE;
        } else {
            return this.defaultMQPushConsumer.getMaxReconsumeTimes();
        }
    }

    /**
     * 判断是否还有重试次数
     *      1、重试未超过最大重试次数 返回true 表示可以重试
     *      2、重试超过最大重试次数 先尝试回退消息到broker
     *              如果回退成功 返回false 不再重试
     *              如果回退失败 则返回true 继续重试
     * @param msgs
     * @return true 表示可以重试  false 不再重试
     */
    private boolean checkReconsumeTimes(List<MessageExt> msgs) {
        boolean suspend = false;
        if (msgs != null && !msgs.isEmpty()) {
            for (MessageExt msg : msgs) {
                //如果重试次数大于最大重试次数 顺序消费默认是int最大值
                if (msg.getReconsumeTimes() >= getMaxReconsumeTimes()) {
                    MessageAccessor.setReconsumeTime(msg, String.valueOf(msg.getReconsumeTimes()));
                    if (!sendMessageBack(msg)) {
                        suspend = true;
                        msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                    }
                } else {
                    suspend = true;
                    //增加消息重试次数
                    msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                }
            }
        }
        return suspend;
    }

    public boolean sendMessageBack(final MessageExt msg) {
        try {
            // max reconsume times exceeded then send to dead letter queue.
            Message newMsg = new Message(MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup()), msg.getBody());
            String originMsgId = MessageAccessor.getOriginMessageId(msg);
            MessageAccessor.setOriginMessageId(newMsg, UtilAll.isBlank(originMsgId) ? msg.getMsgId() : originMsgId);
            newMsg.setFlag(msg.getFlag());
            MessageAccessor.setProperties(newMsg, msg.getProperties());
            MessageAccessor.putProperty(newMsg, MessageConst.PROPERTY_RETRY_TOPIC, msg.getTopic());
            MessageAccessor.setReconsumeTime(newMsg, String.valueOf(msg.getReconsumeTimes()));
            MessageAccessor.setMaxReconsumeTimes(newMsg, String.valueOf(getMaxReconsumeTimes()));
            MessageAccessor.clearProperty(newMsg, MessageConst.PROPERTY_TRANSACTION_PREPARED);
            newMsg.setDelayTimeLevel(3 + msg.getReconsumeTimes());

            this.defaultMQPushConsumer.getDefaultMQPushConsumerImpl().getmQClientFactory().getDefaultMQProducer().send(newMsg);
            return true;
        } catch (Exception e) {
            log.error("sendMessageBack exception, group: " + this.consumerGroup + " msg: " + msg.toString(), e);
        }

        return false;
    }

    public void resetNamespace(final List<MessageExt> msgs) {
        for (MessageExt msg : msgs) {
            if (StringUtils.isNotEmpty(this.defaultMQPushConsumer.getNamespace())) {
                msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQPushConsumer.getNamespace()));
            }
        }
    }

    /**
     * 消费任务
     * 顺序消费不会直接消费本次拉取的消息 而是消息消费时从本地处理队列获取消息
     */
    class ConsumeRequest implements Runnable {
        /**本地处理队列*/
        private final ProcessQueue processQueue;
        /**topic队列*/
        private final MessageQueue messageQueue;

        public ConsumeRequest(ProcessQueue processQueue, MessageQueue messageQueue) {
            this.processQueue = processQueue;
            this.messageQueue = messageQueue;
        }

        public ProcessQueue getProcessQueue() {
            return processQueue;
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

        @Override
        public void run() {
            if (this.processQueue.isDropped()) {
                log.warn("run, the message queue not be able to consume, because it's dropped. {}", this.messageQueue);
                return;
            }

            //获取对应topic队列的本地锁
            final Object objLock = messageQueueLock.fetchLockObject(this.messageQueue);

            //加锁 保证一个topic个队列同一时间只能有一条消息消费，顺序消费必须确保topic队列的消费是顺序的
            synchronized (objLock) {
                /**
                 * 1、广播模式
                 * 2、当前消费者占用topic队列对应的broker端分布式锁 并且 锁没有过期（客户端30s过期）
                 */
                if (MessageModel.BROADCASTING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())
                    || (this.processQueue.isLocked() && !this.processQueue.isLockExpired())) {
                    final long beginTime = System.currentTimeMillis();
                    for (boolean continueConsume = true; continueConsume; ) {

                        /**
                         * 如果ProcessQueue为删除状态就停止消费 说明重平衡后该topic队列被分配给了其它消费者 本地提交消费进度到broker 并且尝试释放broker端对应topic队列的分布式锁
                         */
                        if (this.processQueue.isDropped()) {
                            log.warn("the message queue not be able to consume, because it's dropped. {}", this.messageQueue);
                            break;
                        }

                        /**
                         * 集群模式下，当前消费者未占用topic队列对应的broker端分布式锁 放弃消费
                         */
                        if (MessageModel.CLUSTERING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())
                            && !this.processQueue.isLocked()) {
                            log.warn("the message queue not locked, so consume later, {}", this.messageQueue);
                            ConsumeMessageOrderlyService.this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 10);
                            break;
                        }

                        /**
                         * 集群模式下，当前消费者占用topic队列对应的broker端分布式锁过期 放弃消费
                         */
                        if (MessageModel.CLUSTERING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())
                            && this.processQueue.isLockExpired()) {
                            log.warn("the message queue lock expired, so consume later, {}", this.messageQueue);
                            ConsumeMessageOrderlyService.this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 10);
                            break;
                        }

                        /**
                         * 超过最大消费时间 60s 放弃消费
                         */
                        long interval = System.currentTimeMillis() - beginTime;
                        if (interval > MAX_TIME_CONSUME_CONTINUOUSLY) {
                            ConsumeMessageOrderlyService.this.submitConsumeRequestLater(processQueue, messageQueue, 10);
                            break;
                        }

                        //每次循环处理的消息数量 一般是1
                        final int consumeBatchSize =
                            ConsumeMessageOrderlyService.this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize();

                        //从processQueue中拿消息 并放入consumingMsgOrderlyTreeMap
                        List<MessageExt> msgs = this.processQueue.takeMessages(consumeBatchSize);
                        //如果是重试消息 将重试消息的重试主题（%RETRY% + 消费者组组名）改为原主题
                        defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, defaultMQPushConsumer.getConsumerGroup());
                        if (!msgs.isEmpty()) {
                            //创建消费上下文
                            final ConsumeOrderlyContext context = new ConsumeOrderlyContext(this.messageQueue);

                            /**
                             * 消费状态
                             *      SUCCESS 消费成功
                             *      SUSPEND_CURRENT_QUEUE_A_MOMENT 消费失败，表示失败后需要挂起一小段时间
                             *
                             */
                            ConsumeOrderlyStatus status = null;

                            ConsumeMessageContext consumeMessageContext = null;
                            if (ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                                consumeMessageContext = new ConsumeMessageContext();
                                consumeMessageContext
                                    .setConsumerGroup(ConsumeMessageOrderlyService.this.defaultMQPushConsumer.getConsumerGroup());
                                consumeMessageContext.setNamespace(defaultMQPushConsumer.getNamespace());
                                consumeMessageContext.setMq(messageQueue);
                                consumeMessageContext.setMsgList(msgs);
                                consumeMessageContext.setSuccess(false);
                                // init the consume context type
                                consumeMessageContext.setProps(new HashMap<String, String>());
                                ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.executeHookBefore(consumeMessageContext);
                            }

                            long beginTimestamp = System.currentTimeMillis();
                            ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;
                            //消息消费是否异常
                            boolean hasException = false;
                            try {
                                /**
                                 * 获取ProcessQueue 的consumeLock，消息处理过程中不能被打断，跟RebalanceImpl同步使用
                                 *
                                 *    当重平衡后topic队列被分配给其他消费者后，需要设置对应topic队列ProcessQueue状态为删除，
                                 *          持久化消费进度到mq归属的broker节点，删除topic队列本地的消费进度
                                 *    如果是顺序消费： 需要尝试释放broker端topic队列分布式锁
                                 *            1、获取本地消费队列的锁，设置超时时间为1s
                                 *               2.1 如果获取本地消费队列的consumeLock锁失败，统计尝试获取锁次数
                                 *               2.2 如果获取本地消费队列的consumeLock锁成功，判断ProcessQueue里面有没有数据
                                 *                  2.2.1、ProcessQueue里面没有数据，说明消费者本地消费任务已经退出了，立即释放broker端topic队列分布式锁
                                 *                  2.2.2、ProcessQueue里面有数据，则延时20秒释放broker端topic队列分布式锁
                                 *
                                 */
                                this.processQueue.getConsumeLock().lock();
                                if (this.processQueue.isDropped()) {
                                    log.warn("consumeMessage, the message queue not be able to consume, because it's dropped. {}",
                                        this.messageQueue);
                                    break;
                                }

                                //调用消息监听器处理消息
                                status = messageListener.consumeMessage(Collections.unmodifiableList(msgs), context);
                            } catch (Throwable e) {
                                log.warn("consumeMessage exception: {} Group: {} Msgs: {} MQ: {}",
                                    RemotingHelper.exceptionSimpleDesc(e),
                                    ConsumeMessageOrderlyService.this.consumerGroup,
                                    msgs,
                                    messageQueue);
                                hasException = true;
                            } finally {
                                //消息消费完后释放锁
                                this.processQueue.getConsumeLock().unlock();
                            }

                            if (null == status
                                || ConsumeOrderlyStatus.ROLLBACK == status
                                || ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT == status) {
                                log.warn("consumeMessage Orderly return not OK, Group: {} Msgs: {} MQ: {}",
                                    ConsumeMessageOrderlyService.this.consumerGroup,
                                    msgs,
                                    messageQueue);
                            }

                            long consumeRT = System.currentTimeMillis() - beginTimestamp;
                            if (null == status) {
                                if (hasException) {
                                    returnType = ConsumeReturnType.EXCEPTION;
                                } else {
                                    returnType = ConsumeReturnType.RETURNNULL;
                                }
                            } else if (consumeRT >= defaultMQPushConsumer.getConsumeTimeout() * 60 * 1000) {
                                returnType = ConsumeReturnType.TIME_OUT;
                            } else if (ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT == status) {
                                returnType = ConsumeReturnType.FAILED;
                            } else if (ConsumeOrderlyStatus.SUCCESS == status) {
                                returnType = ConsumeReturnType.SUCCESS;
                            }

                            if (ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                                consumeMessageContext.getProps().put(MixAll.CONSUME_CONTEXT_TYPE, returnType.name());
                            }

                            if (null == status) {
                                status = ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                            }

                            if (ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                                consumeMessageContext.setStatus(status.toString());
                                consumeMessageContext
                                    .setSuccess(ConsumeOrderlyStatus.SUCCESS == status || ConsumeOrderlyStatus.COMMIT == status);
                                ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.executeHookAfter(consumeMessageContext);
                            }

                            ConsumeMessageOrderlyService.this.getConsumerStatsManager()
                                .incConsumeRT(ConsumeMessageOrderlyService.this.consumerGroup, messageQueue.getTopic(), consumeRT);


                            /**
                             * 顺序消费消费结果处理
                             * @param msgs 本轮从从ProcessQueue获取的消息集合（默认1条）
                             * @param status 消费状态
                             * @param context 消费上下文
                             * @param consumeRequest 消费任务
                             * @return
                             */
                            continueConsume = ConsumeMessageOrderlyService.this.processConsumeResult(msgs, status, context, this);
                        } else {
                            //如果从ProcessQueue拿出的一批消息（默认1条）消费完了 就将continueConsume设置为false for循环结束 退出消费
                            continueConsume = false;
                        }
                    }
                } else {
                    if (this.processQueue.isDropped()) {
                        log.warn("the message queue not be able to consume, because it's dropped. {}", this.messageQueue);
                        return;
                    }

                    ConsumeMessageOrderlyService.this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 100);
                }
            }
        }

    }

}
