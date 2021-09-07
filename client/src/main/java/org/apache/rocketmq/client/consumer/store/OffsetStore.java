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
package org.apache.rocketmq.client.consumer.store;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.Map;
import java.util.Set;

/**
 * 消费进度管理接口
 *       广播模式 消费进度保存在本地
 *       集群模式 消费进度保存在broker
 */
public interface OffsetStore {
    /**
     * 从进度存储文件加载消息消费进度到内存
     */
    void load() throws MQClientException;

    /**
     *更新内存中的消息消费进度
     * @Param mq  topic队列
     * @Param offset  偏移量
     * @Param increaseOnly  true时 只有offset大于内存中当前消费偏移量才更新
     * @Return void
     */
    void updateOffset(final MessageQueue mq, final long offset, final boolean increaseOnly);

    /**
     * 读取消费进度
     * @param mq topic队列
     * @param type 读取方式
     *             READ_FROM_MEMORY 从内存读
     *             READ_FROM_STORE  从磁盘读
     *             MEMORY_FIRST_THEN_STORE 先从内存读后从磁盘读
     *
     * @return
     */
    long readOffset(final MessageQueue mq, final ReadOffsetType type);

    /**
     * 持久化多个topic队列的消费进度到磁盘
     * @param mqs topic队列
     */
    void persistAll(final Set<MessageQueue> mqs);


    /**
     * 持久化指定topic队列的消费进度到磁盘
     * @param mq topic队列
     */
    void persist(final MessageQueue mq);

    /**
     *将消息队列的消费进度从内存中移除
     * @Param mq  topic队列
     * @Return void
     */
    void removeOffset(MessageQueue mq);

    /**
     * 克隆某主题下所有topic队列的消费进度
     * @param topic 主题
     * @return
     */
    Map<MessageQueue, Long> cloneOffsetTable(String topic);

    /**
     * 更新存储在broker端的消息消费进度  集群模式下
     * @param mq  topic队列
     * @param offset 消费进度偏移量
     * @param isOneway 是否单向请求
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    void updateConsumeOffsetToBroker(MessageQueue mq, long offset, boolean isOneway) throws RemotingException,
        MQBrokerException, InterruptedException, MQClientException;
}
