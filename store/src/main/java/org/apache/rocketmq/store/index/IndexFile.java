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
package org.apache.rocketmq.store.index;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MappedFile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.List;

public class IndexFile {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    /**每个hash槽大小 4B*/
    private static int hashSlotSize = 4;
    /**每个index条目大小 20B*/
    private static int indexSize = 20;
    /**无效索引编号 特殊值*/
    private static int invalidIndex = 0;
    /**hash槽数量 默认500w*/
    private final int hashSlotNum;
    /**index条目数量 默认2000w*/
    private final int indexNum;
    /**索引文件对象*/
    private final MappedFile mappedFile;
    /**索引文件对象中的fileChannel*/
    private final FileChannel fileChannel;
    /**索引文件对象中的mappedByteBuffer*/
    private final MappedByteBuffer mappedByteBuffer;
    /**索引头对象*/
    private final IndexHeader indexHeader;

    /**
     *
     * @param fileName
     * @param hashSlotNum
     * @param indexNum
     * @param endPhyOffset 上个索引文件最后一条消息的物理偏移量
     * @param endTimestamp 上个索引文件最后一条消息存储时间
     * @throws IOException
     */
    public IndexFile(final String fileName, final int hashSlotNum, final int indexNum,
        final long endPhyOffset, final long endTimestamp) throws IOException {
        //文件总大小 40 + 4 * 500w + 20 * 2000w
        int fileTotalSize =
            IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * hashSlotSize) + (indexNum * indexSize);
        //创建mappedFile
        this.mappedFile = new MappedFile(fileName, fileTotalSize);
        this.fileChannel = this.mappedFile.getFileChannel();
        this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
        this.hashSlotNum = hashSlotNum;
        this.indexNum = indexNum;

        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        this.indexHeader = new IndexHeader(byteBuffer);

        if (endPhyOffset > 0) {
            this.indexHeader.setBeginPhyOffset(endPhyOffset);
            this.indexHeader.setEndPhyOffset(endPhyOffset);
        }

        if (endTimestamp > 0) {
            this.indexHeader.setBeginTimestamp(endTimestamp);
            this.indexHeader.setEndTimestamp(endTimestamp);
        }
    }

    public String getFileName() {
        return this.mappedFile.getFileName();
    }

    public void load() {
        this.indexHeader.load();
    }

    public void flush() {
        long beginTime = System.currentTimeMillis();
        if (this.mappedFile.hold()) {
            //更新索引头到byteBuffer
            this.indexHeader.updateByteBuffer();
            //强制刷盘
            this.mappedByteBuffer.force();
            this.mappedFile.release();
            log.info("flush index file elapsed time(ms) " + (System.currentTimeMillis() - beginTime));
        }
    }

    public boolean isWriteFull() {
        //已使用的index条目大于index条目总数 即写满
        return this.indexHeader.getIndexCount() >= this.indexNum;
    }

    public boolean destroy(final long intervalForcibly) {
        return this.mappedFile.destroy(intervalForcibly);
    }

    /**
     *
     * @param key  (1 UNIQ_KEY  2 KEYS= "a,b,c" 会分别为a,b,c创建索引 )
     * @param phyOffset 消息的物理偏移量
     * @param storeTimestamp 消息的存储时间
     * @return
     */
    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
        //说明index条目还有空间
        if (this.indexHeader.getIndexCount() < this.indexNum) {
            //获取key的hash值 大于0
            int keyHash = indexKeyHashMethod(key);
            //取模获取哈希槽下标
            int slotPos = keyHash % this.hashSlotNum;
            //计算哈希槽的物理起始位置
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;

            try {

                // fileLock = this.fileChannel.lock(absSlotPos, hashSlotSize,
                // false);
                //读取hash桶中的原值 当hash冲突时原桶有值 其他情况是invalidIndex 0
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) {
                    //说明slotValue为无效值
                    slotValue = invalidIndex;
                }

                //当前消息的时间减去 索引文件第一条消息的时间 ，得到差值，使用4byte表示，而当前时间则需要用8byte表示，节省了空间
                long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();

                //转换为秒
                timeDiff = timeDiff / 1000;


                if (this.indexHeader.getBeginTimestamp() <= 0) {
                    //第一条索引插入时 timeDiff=0
                    timeDiff = 0;
                } else if (timeDiff > Integer.MAX_VALUE) {
                    timeDiff = Integer.MAX_VALUE;
                } else if (timeDiff < 0) {
                    timeDiff = 0;
                }

                //索引条目写入时的物理偏移量
                int absIndexPos =
                    IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                        + this.indexHeader.getIndexCount() * indexSize;

                //key的hashcode
                this.mappedByteBuffer.putInt(absIndexPos, keyHash);
                //消息偏移量
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
                //消息相对于索引中第一条消息存储时间的差值
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);
                /**
                 *
                 * ===========解决hash冲突的关键=========
                 * 上一个index条目的编号 如果hash冲突了 那么此时slotValue保存的也是一个index条目的编号
                 * 将hash冲突的hash槽内index条目形成了链式结构
                 */
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);
                //哈希槽内写入索引编号
                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());

                if (this.indexHeader.getIndexCount() <= 1) {
                    //第一条索引数据需要在header插入一些数据
                    this.indexHeader.setBeginPhyOffset(phyOffset);
                    this.indexHeader.setBeginTimestamp(storeTimestamp);
                }

                if (invalidIndex == slotValue) {
                    //slotValue 返回invalidIndex 0 表示占用了一个新桶，占用hash桶数量hashSlotCount +1
                    this.indexHeader.incHashSlotCount();
                }
                //索引条目+1
                this.indexHeader.incIndexCount();
                //设置最大消息的物理偏移量
                this.indexHeader.setEndPhyOffset(phyOffset);
                //设置最大消息的存储时间
                this.indexHeader.setEndTimestamp(storeTimestamp);

                return true;
            } catch (Exception e) {
                log.error("putKey exception, Key: " + key + " KeyHashCode: " + key.hashCode(), e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }
            }
        } else {
            log.warn("Over index file capacity: index count = " + this.indexHeader.getIndexCount()
                + "; index max num = " + this.indexNum);
        }

        return false;
    }

    public int indexKeyHashMethod(final String key) {
        int keyHash = key.hashCode();
        int keyHashPositive = Math.abs(keyHash);
        if (keyHashPositive < 0)
            keyHashPositive = 0;
        return keyHashPositive;
    }

    public long getBeginTimestamp() {
        return this.indexHeader.getBeginTimestamp();
    }

    public long getEndTimestamp() {
        return this.indexHeader.getEndTimestamp();
    }

    public long getEndPhyOffset() {
        return this.indexHeader.getEndPhyOffset();
    }

    /**
     * 判断时间范围是否有交集
     * @param begin
     * @param end
     * @return
     */
    public boolean isTimeMatched(final long begin, final long end) {
        boolean result = begin < this.indexHeader.getBeginTimestamp() && end > this.indexHeader.getEndTimestamp();
        result = result || (begin >= this.indexHeader.getBeginTimestamp() && begin <= this.indexHeader.getEndTimestamp());
        result = result || (end >= this.indexHeader.getBeginTimestamp() && end <= this.indexHeader.getEndTimestamp());
        return result;
    }

    /**
     *根据key 和时间返回查询消息
     * @param phyOffsets 查询结果
     * @param key 查询key
     * @param maxNum 查询最大数量
     * @param begin 开始时间戳
     * @param end  结束时间戳
     * @param lock
     */
    public void selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum,
        final long begin, final long end, boolean lock) {
        //引用次数+1
        if (this.mappedFile.hold()) {
            //获取key的hash值
            int keyHash = indexKeyHashMethod(key);
            //取模获取哈希槽下标
            int slotPos = keyHash % this.hashSlotNum;
            //取模获取哈希槽物理起始位置
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;
            try {
                if (lock) {
                    // fileLock = this.fileChannel.lock(absSlotPos,
                    // hashSlotSize, true);
                }

                //获取hash通里的值 可能为无效值 invalidIndex 0 也有可能为index条目编号
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                // if (fileLock != null) {
                // fileLock.release();
                // fileLock = null;
                // }

                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()
                    || this.indexHeader.getIndexCount() <= 1) {
                    //查询未命中
                } else {
                    //index条目编号
                    for (int nextIndexToRead = slotValue; ; ) {
                        if (phyOffsets.size() >= maxNum) {
                            //数量是否达到了最大值
                            break;
                        }

                        //获取index条目的物理位置
                        int absIndexPos =
                            IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                                + nextIndexToRead * indexSize;

                        //index条目中存储的key的hash值
                        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                        //index条目中存储的消息的物理偏移量
                        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);
                        //index条目中存储的消息和索引文件第一条消息存储时间差
                        long timeDiff = (long) this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);

                        //该index条目的下一个索引条目编号
                        //没有hash冲突时 prevIndexRead为无效值 invalidIndex 0 存在hash冲突时 为冲突时原桶对应的index条目编号
                        int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);

                        if (timeDiff < 0) {
                            break;
                        }

                        timeDiff *= 1000L;

                        long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;
                        //判断时间是否符合查询条件
                        boolean timeMatched = (timeRead >= begin) && (timeRead <= end);

                        //如果hash相同并且时间范围匹配 则证明是要查找的消息
                        if (keyHash == keyHashRead && timeMatched) {
                            //加入到结果集合
                            phyOffsets.add(phyOffsetRead);
                        }

                        if (prevIndexRead <= invalidIndex
                            || prevIndexRead > this.indexHeader.getIndexCount()
                            || prevIndexRead == nextIndexToRead || timeRead < begin) {
                            //证明 没有发生hash冲突 命中index条目没有关联index条目
                            break;
                        }

                        //说明发生了hash冲突，需要判断同一链上的index条目是否为查询目标消息
                        nextIndexToRead = prevIndexRead;
                    }
                }
            } catch (Exception e) {
                log.error("selectPhyOffset exception ", e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }

                this.mappedFile.release();
            }
        }
    }
}
