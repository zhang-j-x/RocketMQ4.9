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

import org.apache.rocketmq.client.MQAdmin;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.Set;

/**
 * Message queue consumer interface
 */
public interface MQConsumer extends MQAdmin {
    /**
     * 消息消费失败重新发送到Broker服务器
     * @param msg 消息
     * @param delayLevel 消息延迟级别
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    @Deprecated
    void sendMessageBack(final MessageExt msg, final int delayLevel) throws RemotingException,
        MQBrokerException, InterruptedException, MQClientException;

    /**
     * 消息消费失败重新发送到Broker服务器
     * @param msg 消息
     * @param delayLevel 消息延迟级别
     * @param brokerName broker名称
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    void sendMessageBack(final MessageExt msg, final int delayLevel, final String brokerName)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

    /**
     * 获取主题topic分配了消费者哪些消息队列
     * @param topic 主题
     * @return 队列信息
     * @throws MQClientException
     */
    Set<MessageQueue> fetchSubscribeMessageQueues(final String topic) throws MQClientException;
}
