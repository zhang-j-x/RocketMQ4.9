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
package org.apache.rocketmq.broker.client.rebalance;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class RebalanceLockManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.REBALANCE_LOCK_LOGGER_NAME);
    private final static long REBALANCE_LOCK_MAX_LIVE_TIME = Long.parseLong(System.getProperty(
        "rocketmq.broker.rebalance.lockMaxLiveTime", "60000"));
    private final Lock lock = new ReentrantLock();
    /**
     * 消费者组 -> (消费队列 -> 锁对象)
     */
    private final ConcurrentMap<String/* group */, ConcurrentHashMap<MessageQueue, LockEntry>> mqLockTable =
        new ConcurrentHashMap<String, ConcurrentHashMap<MessageQueue, LockEntry>>(1024);

    /**
     *
     * @param group 消费组
     * @param mq 加锁的消费队列
     * @param clientId 加锁的客户端id
     * @return
     *      加锁成功的情况：
     *          1、没有对应锁对象，当前消费者是第一个获取锁的，加锁成功
     *          2、请求加锁的客户端和持有锁的客户端相同且锁未过期，则锁续约成功
     *          3、请求加锁的客户端和持有锁的客户端相同且但锁过期（60s），打印告警日志，锁续约成功
     *          4、消费队列持有锁的客户端和请求加锁的客户端不同，加锁失败
     */
    public boolean tryLock(final String group, final MessageQueue mq, final String clientId) {

        if (!this.isLocked(group, mq, clientId)) {
            try {
                //直接加锁 避免并发问题
                this.lock.lockInterruptibly();
                try {
                    //获取消费组的锁信息
                    ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
                    if (null == groupValue) {
                        groupValue = new ConcurrentHashMap<>(32);
                        this.mqLockTable.put(group, groupValue);
                    }

                    LockEntry lockEntry = groupValue.get(mq);
                    //如果锁对象都没有 说明当前消费者是第一个获取锁的，加锁成功
                    if (null == lockEntry) {
                        lockEntry = new LockEntry();
                        lockEntry.setClientId(clientId);
                        groupValue.put(mq, lockEntry);
                        log.info("tryLock, message queue not locked, I got it. Group: {} NewClientId: {} {}",
                            group,
                            clientId,
                            mq);
                    }

                    //如果请求加锁的客户端和持有锁的客户端相同且锁未过期，则锁续约成功
                    if (lockEntry.isLocked(clientId)) {
                        lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                        return true;
                    }

                    /**
                     * 走到这里有两种情况
                     * 1、当前消费队列持有锁的客户端和请求加锁的客户端不同
                     * 2、请求加锁的客户端和持有锁的客户端相同但是锁过期了
                     */

                    //旧的客户端id
                    String oldClientId = lockEntry.getClientId();

                    //锁过期了 只是打印了日志 但是还是会获取锁成功
                    if (lockEntry.isExpired()) {
                        lockEntry.setClientId(clientId);
                        lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                        log.warn(
                            "tryLock, message queue lock expired, I got it. Group: {} OldClientId: {} NewClientId: {} {}",
                            group,
                            oldClientId,
                            clientId,
                            mq);
                        return true;
                    }

                    /**
                     * 走到这里就说明 当前消费队列持有锁的客户端和请求加锁的客户端不同 返回加锁失败
                     */
                    log.warn(
                        "tryLock, message queue locked by other client. Group: {} OtherClientId: {} NewClientId: {} {}",
                        group,
                        oldClientId,
                        clientId,
                        mq);
                    return false;
                } finally {
                    this.lock.unlock();
                }
            } catch (InterruptedException e) {
                log.error("putMessage exception", e);
            }
        } else {

        }

        return true;
    }

    /**
     * 判断请求加锁的客户端和持有锁的客户端是否相同且锁未过期，是则直接更改获取锁时间
     * @param group
     * @param mq
     * @param clientId
     * @return
     */
    private boolean isLocked(final String group, final MessageQueue mq, final String clientId) {
        ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
        if (groupValue != null) {
            LockEntry lockEntry = groupValue.get(mq);
            if (lockEntry != null) {
                //请求加锁的客户端和持有锁的客户端相同且锁未过期  则返回true
                boolean locked = lockEntry.isLocked(clientId);
                if (locked) {
                    lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                }

                return locked;
            }
        }

        return false;
    }

    /**
     * 对一批消费队列加锁
     * @param group 消费者组
     * @param mqs 消费队列
     * @param clientId 客户端id
     * @return 返回加锁成功的消费队列
     */
    public Set<MessageQueue> tryLockBatch(final String group, final Set<MessageQueue> mqs,
        final String clientId) {
        Set<MessageQueue> lockedMqs = new HashSet<MessageQueue>(mqs.size());
        Set<MessageQueue> notLockedMqs = new HashSet<MessageQueue>(mqs.size());

        for (MessageQueue mq : mqs) {
            if (this.isLocked(group, mq, clientId)) {
                /**
                 * 请求加锁的客户端和持有锁的客户端相同且锁未过期的消费队列
                 */
                lockedMqs.add(mq);
            } else {
                notLockedMqs.add(mq);
            }
        }

        /**
         * 1、请求加锁的客户端和持有锁的客户端不同
         * 2、请求加锁的客户端和持有锁的客户端相同但锁未期
         * 3、对应消费队列没有锁对象，为第一次获取锁
         */
        if (!notLockedMqs.isEmpty()) {
            try {
                this.lock.lockInterruptibly();
                try {
                    ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
                    if (null == groupValue) {
                        groupValue = new ConcurrentHashMap<>(32);
                        this.mqLockTable.put(group, groupValue);
                    }

                    for (MessageQueue mq : notLockedMqs) {
                        LockEntry lockEntry = groupValue.get(mq);
                        //没有锁对象 第一次拿锁 成功
                        if (null == lockEntry) {
                            lockEntry = new LockEntry();
                            lockEntry.setClientId(clientId);
                            groupValue.put(mq, lockEntry);
                            log.info(
                                "tryLockBatch, message queue not locked, I got it. Group: {} NewClientId: {} {}",
                                group,
                                clientId,
                                mq);
                        }

                        if (lockEntry.isLocked(clientId)) {
                            lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                            lockedMqs.add(mq);
                            continue;
                        }

                        String oldClientId = lockEntry.getClientId();

                        //请求加锁的客户端和持有锁的客户端相同但锁未期 大于告警日志 但是还是获取锁成功
                        if (lockEntry.isExpired()) {
                            lockEntry.setClientId(clientId);
                            lockEntry.setLastUpdateTimestamp(System.currentTimeMillis());
                            log.warn(
                                "tryLockBatch, message queue lock expired, I got it. Group: {} OldClientId: {} NewClientId: {} {}",
                                group,
                                oldClientId,
                                clientId,
                                mq);
                            lockedMqs.add(mq);
                            continue;
                        }

                        //请求加锁的客户端和持有锁的客户端不同的加锁失败
                        log.warn(
                            "tryLockBatch, message queue locked by other client. Group: {} OtherClientId: {} NewClientId: {} {}",
                            group,
                            oldClientId,
                            clientId,
                            mq);
                    }
                } finally {
                    this.lock.unlock();
                }
            } catch (InterruptedException e) {
                log.error("putMessage exception", e);
            }
        }

        return lockedMqs;
    }

    /**
     * 批量解锁
     * @param group 消费者组
     * @param mqs 消费队列
     * @param clientId 客户端id
     */
    public void unlockBatch(final String group, final Set<MessageQueue> mqs, final String clientId) {
        try {
            this.lock.lockInterruptibly();
            try {
                ConcurrentHashMap<MessageQueue, LockEntry> groupValue = this.mqLockTable.get(group);
                if (null != groupValue) {
                    for (MessageQueue mq : mqs) {
                        LockEntry lockEntry = groupValue.get(mq);
                        if (null != lockEntry) {
                            //如果锁由当前客户端持有 移除锁
                            if (lockEntry.getClientId().equals(clientId)) {
                                groupValue.remove(mq);
                                log.info("unlockBatch, Group: {} {} {}",
                                    group,
                                    mq,
                                    clientId);
                            } else {
                                log.warn("unlockBatch, but mq locked by other client: {}, Group: {} {} {}",
                                    lockEntry.getClientId(),
                                    group,
                                    mq,
                                    clientId);
                            }
                        } else {
                            log.warn("unlockBatch, but mq not locked, Group: {} {} {}",
                                group,
                                mq,
                                clientId);
                        }
                    }
                } else {
                    log.warn("unlockBatch, group not exist, Group: {} {}",
                        group,
                        clientId);
                }
            } finally {
                this.lock.unlock();
            }
        } catch (InterruptedException e) {
            log.error("putMessage exception", e);
        }
    }

    static class LockEntry {
        /**客户端id*/
        private String clientId;
        /**更新时间*/
        private volatile long lastUpdateTimestamp = System.currentTimeMillis();

        public String getClientId() {
            return clientId;
        }

        public void setClientId(String clientId) {
            this.clientId = clientId;
        }

        public long getLastUpdateTimestamp() {
            return lastUpdateTimestamp;
        }

        public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
            this.lastUpdateTimestamp = lastUpdateTimestamp;
        }

        /**
         * 请求加锁的客户端和持有锁的客户端相同且锁未过期
         * @param clientId 客户端id
         * @return
         */
        public boolean isLocked(final String clientId) {
            boolean eq = this.clientId.equals(clientId);
            return eq && !this.isExpired();
        }

        /**
         * 锁是否过期
         *  当前时间-上次获取锁时间是否超过 60s
         * @return
         */
        public boolean isExpired() {
            boolean expired =
                (System.currentTimeMillis() - this.lastUpdateTimestamp) > REBALANCE_LOCK_MAX_LIVE_TIME;

            return expired;
        }
    }
}
