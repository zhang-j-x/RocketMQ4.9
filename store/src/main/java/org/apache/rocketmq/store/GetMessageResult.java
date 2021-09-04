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

import org.apache.rocketmq.store.stats.BrokerStatsManager;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class GetMessageResult {

    /**查询消息是底层是MappedFile切片返回给外层SelectMappedBufferResult，MappedFile每切片一次refCount++,需要通过
     * SelectMappedBufferResult释放refCount
     * */
    private final List<SelectMappedBufferResult> messageMapedList =
        new ArrayList<SelectMappedBufferResult>(100);

    /**存储消息*/
    private final List<ByteBuffer> messageBufferList = new ArrayList<ByteBuffer>(100);

    private GetMessageStatus status;
    /**当前topic队列下次拉消息是的偏移量*/
    private long nextBeginOffset;
    /**当前topic队列最小偏移量*/
    private long minOffset;
    /**当前topic队列最大偏移量*/
    private long maxOffset;
    /**消息总大小*/
    private int bufferTotalSize = 0;

    /**服务端是否建议下次拉取该topic队列消息去从节点*/
    private boolean suggestPullingFromSlave = false;

    private int msgCount4Commercial = 0;

    public GetMessageResult() {
    }

    public GetMessageStatus getStatus() {
        return status;
    }

    public void setStatus(GetMessageStatus status) {
        this.status = status;
    }

    public long getNextBeginOffset() {
        return nextBeginOffset;
    }

    public void setNextBeginOffset(long nextBeginOffset) {
        this.nextBeginOffset = nextBeginOffset;
    }

    public long getMinOffset() {
        return minOffset;
    }

    public void setMinOffset(long minOffset) {
        this.minOffset = minOffset;
    }

    public long getMaxOffset() {
        return maxOffset;
    }

    public void setMaxOffset(long maxOffset) {
        this.maxOffset = maxOffset;
    }

    public List<SelectMappedBufferResult> getMessageMapedList() {
        return messageMapedList;
    }

    public List<ByteBuffer> getMessageBufferList() {
        return messageBufferList;
    }

    public void addMessage(final SelectMappedBufferResult mapedBuffer) {
        this.messageMapedList.add(mapedBuffer);
        this.messageBufferList.add(mapedBuffer.getByteBuffer());
        this.bufferTotalSize += mapedBuffer.getSize();
        this.msgCount4Commercial += (int) Math.ceil(
            mapedBuffer.getSize() / BrokerStatsManager.SIZE_PER_COUNT);
    }

    public void release() {
        for (SelectMappedBufferResult select : this.messageMapedList) {
            select.release();
        }
    }

    public int getBufferTotalSize() {
        return bufferTotalSize;
    }

    public void setBufferTotalSize(int bufferTotalSize) {
        this.bufferTotalSize = bufferTotalSize;
    }

    public int getMessageCount() {
        return this.messageMapedList.size();
    }

    public boolean isSuggestPullingFromSlave() {
        return suggestPullingFromSlave;
    }

    public void setSuggestPullingFromSlave(boolean suggestPullingFromSlave) {
        this.suggestPullingFromSlave = suggestPullingFromSlave;
    }

    public int getMsgCount4Commercial() {
        return msgCount4Commercial;
    }

    public void setMsgCount4Commercial(int msgCount4Commercial) {
        this.msgCount4Commercial = msgCount4Commercial;
    }

    @Override
    public String toString() {
        return "GetMessageResult [status=" + status + ", nextBeginOffset=" + nextBeginOffset + ", minOffset="
            + minOffset + ", maxOffset=" + maxOffset + ", bufferTotalSize=" + bufferTotalSize
            + ", suggestPullingFromSlave=" + suggestPullingFromSlave + "]";
    }

}
