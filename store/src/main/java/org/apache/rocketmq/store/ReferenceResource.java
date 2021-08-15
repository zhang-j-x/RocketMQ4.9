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
package org.apache.rocketmq.store;

import java.util.concurrent.atomic.AtomicLong;

public abstract class ReferenceResource {
    /**引用计数*/
    protected final AtomicLong refCount = new AtomicLong(1);
    /**是否存活 默认值true  调用shutdown后设置为false*/
    protected volatile boolean available = true;
    /**是否清理 默认为false 执行完子类的cleanUp方法后，设置为true 表明资源全部释放*/
    protected volatile boolean cleanupOver = false;
    /**第一次关闭资源的时间*/
    private volatile long firstShutdownTimestamp = 0;

    /**
     * 添加引用计数 如果资源可用 引用计数增加 返回true 否则返回false
     * @return
     */
    public synchronized boolean hold() {
        if (this.isAvailable()) {
            if (this.refCount.getAndIncrement() > 0) {
                return true;
            } else {
                this.refCount.getAndDecrement();
            }
        }

        return false;
    }

    /**
     * 资源是否可用
     * @return
     */
    public boolean isAvailable() {
        return this.available;
    }

    public void shutdown(final long intervalForcibly) {
        //初次调用this.available为true  调用shutdown后置为false
        if (this.available) {
            this.available = false;
            this.firstShutdownTimestamp = System.currentTimeMillis();
            //只有在引用计数为0时才会释放资源
            this.release();
        } else if (this.getRefCount() > 0) {
            //如果当前时间和第一次尝试关闭的时间时间差超过了关闭的最大等待时间 每执行一次 引用计数减1000，直到释放资源
            if ((System.currentTimeMillis() - this.firstShutdownTimestamp) >= intervalForcibly) {
                this.refCount.set(-1000 - this.getRefCount());
                this.release();
            }
        }
    }

    public void release() {
        long value = this.refCount.decrementAndGet();
        if (value > 0)
            return;

        synchronized (this) {

            this.cleanupOver = this.cleanup(value);
        }
    }

    public long getRefCount() {
        return this.refCount.get();
    }

    public abstract boolean cleanup(final long currentRef);

    public boolean isCleanupOver() {
        return this.refCount.get() <= 0 && this.cleanupOver;
    }
}
