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
package org.apache.rocketmq.client.consumer;

import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;

/**
 * Push consumer
 */
public interface MQPushConsumer extends MQConsumer {
    /**
     * Start the consumer
     */
    void start() throws MQClientException;

    /**
     * Shutdown the consumer
     */
    void shutdown();

    /**
     * Register the message listener
     */
    @Deprecated
    void registerMessageListener(MessageListener messageListener);

    /**
     * 注册并发消息事件监听器
     * @param messageListener
     */
    void registerMessageListener(final MessageListenerConcurrently messageListener);

    /**
     * 注册顺序消息事件监听器
     * @param messageListener
     */
    void registerMessageListener(final MessageListenerOrderly messageListener);

    /**
     * 基于主题订阅消息
     * @param topic 主题
     * @param subExpression 消息过滤表达式 TAG或者SQL92表达式
     * @throws MQClientException
     */
    void subscribe(final String topic, final String subExpression) throws MQClientException;

    /**
     * 基于主题订阅消息 消息过滤方式使用类模式
     * @param topic 主题
     * @param fullClassName 过滤类全路径名
     * @param filterClassSource 过滤类代码
     * @throws MQClientException
     */
    @Deprecated
    void subscribe(final String topic, final String fullClassName,
        final String filterClassSource) throws MQClientException;

    /**
     * Subscribe some topic with selector.
     * <p>
     * This interface also has the ability of {@link #subscribe(String, String)},
     * and, support other message selection, such as {@link org.apache.rocketmq.common.filter.ExpressionType#SQL92}.
     * </p>
     * <p/>
     * <p>
     * Choose Tag: {@link MessageSelector#byTag(java.lang.String)}
     * </p>
     * <p/>
     * <p>
     * Choose SQL92: {@link MessageSelector#bySql(java.lang.String)}
     * </p>
     *
     * @param selector message selector({@link MessageSelector}), can be null.
     */
    /**
     * 基于主题订阅消息 消息过滤方式使用选择器
     * @param topic 主题
     * @param selector TAG或者SQL92表达式选择器
     * @throws MQClientException
     */
    void subscribe(final String topic, final MessageSelector selector) throws MQClientException;

    /**
     * 取消消息订阅
     * @param topic 主题
     */
    void unsubscribe(final String topic);

    /**
     * 动态更新消费者线程池线程数量
     */
    void updateCorePoolSize(int corePoolSize);

    /**
     * 暂停消费
     */
    void suspend();

    /**
     * 恢复消费
     */
    void resume();
}
