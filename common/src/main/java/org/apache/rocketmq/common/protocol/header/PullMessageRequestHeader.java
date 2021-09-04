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

/**
 * $Id: PullMessageRequestHeader.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.apache.rocketmq.common.protocol.header;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.annotation.CFNullable;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class PullMessageRequestHeader implements CommandCustomHeader {
    @CFNotNull
    /**消费组*/
    private String consumerGroup;
    @CFNotNull
    /**主题*/
    private String topic;
    @CFNotNull
    /**topic队列id*/
    private Integer queueId;
    @CFNotNull
    /**待拉取的下一条消息的offset*/
    private Long queueOffset;
    @CFNotNull
    /**拉消息的数量 默认32*/
    private Integer maxMsgNums;
    @CFNotNull
    /**
     *高4位未使用
     *低4位：
     *    第1位：是否提交消费者本地该队列的offset （一般是1）
     *    第2位：是否允许服务器端长轮询 （一般是1）
     *    第3位：是否提交消费者本地该主题的订阅信息 （一般是0）
     *    第4位：是否为类过滤 （一般是0）
     */
    private Integer sysFlag;
    @CFNotNull
    /**本地该队列的消费进度*/
    private Long commitOffset;
    @CFNotNull
    /**服务器长轮询时 连接最长保持时间 15s*/
    private Long suspendTimeoutMillis;
    @CFNullable
    /**客户端topic订阅数据中的过滤表达式*/
    private String subscription;
    @CFNotNull
    /**客户端topic订阅数据的版本*/
    private Long subVersion;
    /**表达式类型 分为TAG和SQL92*/
    private String expressionType;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getQueueId() {
        return queueId;
    }

    public void setQueueId(Integer queueId) {
        this.queueId = queueId;
    }

    public Long getQueueOffset() {
        return queueOffset;
    }

    public void setQueueOffset(Long queueOffset) {
        this.queueOffset = queueOffset;
    }

    public Integer getMaxMsgNums() {
        return maxMsgNums;
    }

    public void setMaxMsgNums(Integer maxMsgNums) {
        this.maxMsgNums = maxMsgNums;
    }

    public Integer getSysFlag() {
        return sysFlag;
    }

    public void setSysFlag(Integer sysFlag) {
        this.sysFlag = sysFlag;
    }

    public Long getCommitOffset() {
        return commitOffset;
    }

    public void setCommitOffset(Long commitOffset) {
        this.commitOffset = commitOffset;
    }

    public Long getSuspendTimeoutMillis() {
        return suspendTimeoutMillis;
    }

    public void setSuspendTimeoutMillis(Long suspendTimeoutMillis) {
        this.suspendTimeoutMillis = suspendTimeoutMillis;
    }

    public String getSubscription() {
        return subscription;
    }

    public void setSubscription(String subscription) {
        this.subscription = subscription;
    }

    public Long getSubVersion() {
        return subVersion;
    }

    public void setSubVersion(Long subVersion) {
        this.subVersion = subVersion;
    }

    public String getExpressionType() {
        return expressionType;
    }

    public void setExpressionType(String expressionType) {
        this.expressionType = expressionType;
    }
}
