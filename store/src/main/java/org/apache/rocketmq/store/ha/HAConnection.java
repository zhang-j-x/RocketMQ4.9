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
package org.apache.rocketmq.store.ha;

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.SelectMappedBufferResult;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

public class HAConnection {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private final HAService haService;
    /**master和slave 连接通道*/
    private final SocketChannel socketChannel;
    /**slave地址*/
    private final String clientAddr;
    /**写数据服务*/
    private WriteSocketService writeSocketService;
    /**读数据服务*/
    private ReadSocketService readSocketService;

    //slave将要同步的消息偏移量
    private volatile long slaveRequestOffset = -1;
    //slave 上报的最新同步进度 slaveAckOffset之前的消息可以认为slave已经同步完成
    private volatile long slaveAckOffset = -1;

    public HAConnection(final HAService haService, final SocketChannel socketChannel) throws IOException {
        this.haService = haService;
        this.socketChannel = socketChannel;
        this.clientAddr = this.socketChannel.socket().getRemoteSocketAddress().toString();
        this.socketChannel.configureBlocking(false);
        this.socketChannel.socket().setSoLinger(false, -1);
        this.socketChannel.socket().setTcpNoDelay(true);
        //socket接收缓冲区 64k
        this.socketChannel.socket().setReceiveBufferSize(1024 * 64);
        //socket发送缓冲区 64k
        this.socketChannel.socket().setSendBufferSize(1024 * 64);
        //创建读写服务
        this.writeSocketService = new WriteSocketService(this.socketChannel);
        this.readSocketService = new ReadSocketService(this.socketChannel);
        //HAService 中slave连接数量connectionCount自增
        this.haService.getConnectionCount().incrementAndGet();
    }

    public void start() {
        //读写服务启动
        this.readSocketService.start();
        this.writeSocketService.start();
    }

    public void shutdown() {
        this.writeSocketService.shutdown(true);
        this.readSocketService.shutdown(true);
        this.close();
    }

    public void close() {
        if (this.socketChannel != null) {
            try {
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }
        }
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    /**
     * 读数据服务
     */
    class ReadSocketService extends ServiceThread {
        /**读缓冲区大小 1M*/
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024;
        /**选择器*/
        private final Selector selector;
        /**slave连接通道*/
        private final SocketChannel socketChannel;
        /**读缓冲区*/
        private final ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
        /**处理指针*/
        private int processPosition = 0;
        private volatile long lastReadTimestamp = System.currentTimeMillis();

        public ReadSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = RemotingUtil.openSelector();
            this.socketChannel = socketChannel;
            //slave连接通道注册到选择器并监听读事件
            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
            this.setDaemon(true);
        }

        @Override
        public void run() {
            HAConnection.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    //选择器阻塞1000
                    this.selector.select(1000);
                    /**
                     * 执行到此处的情况
                     * 1、通道有读事件就绪
                     * 2、选择器阻塞超时
                     */

                    //处理读事件
                    boolean ok = this.processReadEvent();
                    if (!ok) {
                        HAConnection.log.error("processReadEvent error");
                        break;
                    }

                    //如果长时间没有接收到slave的数据 就会退出循环关闭连接
                    long interval = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastReadTimestamp;
                    if (interval > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaHousekeepingInterval()) {
                        log.warn("ha housekeeping, found this connection[" + HAConnection.this.clientAddr + "] expired, " + interval);
                        break;
                    }
                } catch (Exception e) {
                    HAConnection.log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            //设置读服务的停止标记stopped为true
            this.makeStop();
            //关闭写服务
            writeSocketService.makeStop();
            //从HAService的连接集合connectionList中移除当前连接对象
            haService.removeConnection(HAConnection.this);
            //HAService的连接数量 -1
            HAConnection.this.haService.getConnectionCount().decrementAndGet();
            //取消注册的可读事件
            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                //关闭选择器
                this.selector.close();
                //关闭slave连接
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }

            HAConnection.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return ReadSocketService.class.getSimpleName();
        }

        /**
         * 处理slave发送给master的数据
         * @return
         */
        private boolean processReadEvent() {
            //连续三次没有读取到数据 退出选好
            int readSizeZeroTimes = 0;

            /**
             * !this.byteBufferRead.hasRemaining() 成立说明byteBufferRead已经全部用完
             *          没有剩余的空间了
             *
             */
            if (!this.byteBufferRead.hasRemaining()) {
                //position置零
                this.byteBufferRead.flip();
                //处理位点归零
                this.processPosition = 0;
            }

            while (this.byteBufferRead.hasRemaining()) {
                try {
                    /**
                     * 读取slave发送到master数据
                     *
                     *  slave向master发送的数据格式 {[long][long][long]}  发送slave本地的同步进度
                     */
                    int readSize = this.socketChannel.read(this.byteBufferRead);
                    if (readSize > 0) {
                        readSizeZeroTimes = 0;
                        //更改lastReadTimestamp
                        this.lastReadTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                        //表明至少有一条完整数据
                        if ((this.byteBufferRead.position() - this.processPosition) >= 8) {
                            //表示byteBufferRead中可读数据中 最后一个完整数据的结束位置
                            int pos = this.byteBufferRead.position() - (this.byteBufferRead.position() % 8);
                            //读取最新的slave同步进度
                            long readOffset = this.byteBufferRead.getLong(pos - 8);
                            this.processPosition = pos;
                            //更新slaveAckOffset
                            HAConnection.this.slaveAckOffset = readOffset;

                            /**
                             * slaveRequestOffset < 0，说明服务端不清楚slave需要同步什么位置的数据，
                             *          需要将slave端的同步进度作为下次同步的起始位置， 写数据服务需要使用
                             *
                             */
                            if (HAConnection.this.slaveRequestOffset < 0) {
                                HAConnection.this.slaveRequestOffset = readOffset;
                                log.info("slave[" + HAConnection.this.clientAddr + "] request offset " + readOffset);
                            }

                            //唤醒阻塞的GroupTransferService线程 进而唤醒消息发送线程
                            HAConnection.this.haService.notifyTransferSome(HAConnection.this.slaveAckOffset);
                        }
                    } else if (readSize == 0) {
                        //未读取到新数据  超过三次 退出循环
                        if (++readSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        log.error("read socket[" + HAConnection.this.clientAddr + "] < 0");
                        return false;
                    }
                } catch (IOException e) {
                    log.error("processReadEvent exception", e);
                    return false;
                }
            }

            return true;
        }
    }

    /**
     * 写数据服务
     */
    class WriteSocketService extends ServiceThread {
        /**选择器*/
        private final Selector selector;
        /**slave连接通道*/
        private final SocketChannel socketChannel;
        /**master向slave传输数据的头 消息物理偏移量 + 数据块大小*/
        private final int headerSize = 8 + 4;
        /**header对应的ByteBuffer*/
        private final ByteBuffer byteBufferHeader = ByteBuffer.allocate(headerSize);
        /**下一次master要向slave同步消息的起始位置*/
        private long nextTransferFromWhere = -1;
        /**MappedFile查询对象*/
        private SelectMappedBufferResult selectMappedBufferResult;
        /**上轮数据是否传输完毕*/
        private boolean lastWriteOver = true;
        /**上次写时间戳*/
        private long lastWriteTimestamp = System.currentTimeMillis();

        public WriteSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = RemotingUtil.openSelector();
            this.socketChannel = socketChannel;
            //注册通道到选择器 监听可写事件
            this.socketChannel.register(this.selector, SelectionKey.OP_WRITE);
            this.setDaemon(true);
        }

        @Override
        public void run() {
            HAConnection.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    //选择器阻塞1s
                    this.selector.select(1000);
                    /**
                     * 执行到这里的情况
                     *  1、socket缓冲区有空间可写
                     *  2、选择器阻塞超时
                     */

                    //如果slave将要同步的消息偏移量为-1，即建立连接后slave还没向master发送同步的进度，休眠10s 继续循环
                    if (-1 == HAConnection.this.slaveRequestOffset) {
                        Thread.sleep(10);
                        continue;
                    }

                    if (-1 == this.nextTransferFromWhere) {
                        //初始化nextTransferFromWhere

                        //如果slaveRequestOffset 是0（slave是新节点） 则从master最后一个写的commitlog的MappedFile开始同步数据
                        if (0 == HAConnection.this.slaveRequestOffset) {


                            //获取master最大的消息物理偏移量
                            long masterOffset = HAConnection.this.haService.getDefaultMessageStore().getCommitLog().getMaxOffset();

                            //获取消息对应commitlog 文件MappedFile的偏移量
                            masterOffset =
                                masterOffset
                                    - (masterOffset % HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig()
                                    .getMappedFileSizeCommitLog());

                            if (masterOffset < 0) {
                                masterOffset = 0;
                            }

                            this.nextTransferFromWhere = masterOffset;
                        } else {
                            //使用slaveRequestOffset
                            this.nextTransferFromWhere = HAConnection.this.slaveRequestOffset;
                        }

                        log.info("master transfer data from " + this.nextTransferFromWhere + " to slave[" + HAConnection.this.clientAddr
                            + "], and slave request " + HAConnection.this.slaveRequestOffset);
                    }

                    //上次是否发送完了
                    if (this.lastWriteOver) {

                        long interval =
                            HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastWriteTimestamp;

                        if (interval > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig()
                            .getHaSendHeartbeatInterval()) {

                            // 发送一个header当做心跳 维持长链接
                            this.byteBufferHeader.position(0);
                            this.byteBufferHeader.limit(headerSize);
                            this.byteBufferHeader.putLong(this.nextTransferFromWhere);
                            //数据块大小为0  空的数据包
                            this.byteBufferHeader.putInt(0);
                            this.byteBufferHeader.flip();

                            this.lastWriteOver = this.transferData();
                            if (!this.lastWriteOver)
                                continue;
                        }
                    } else {
                        //上一次的待发送数据未发送完
                        this.lastWriteOver = this.transferData();
                        if (!this.lastWriteOver)
                            continue;
                    }

                    //查询commitlog
                    SelectMappedBufferResult selectResult =
                        HAConnection.this.haService.getDefaultMessageStore().getCommitLogData(this.nextTransferFromWhere);
                    if (selectResult != null) {
                        int size = selectResult.getSize();
                        //如果size大于最大数据包大小 32k，则只能发送32k大小数据
                        if (size > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize()) {
                            size = HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize();
                        }

                        long thisOffset = this.nextTransferFromWhere;
                        //更新下次传输位置
                        this.nextTransferFromWhere += size;
                        //只读取size的数据
                        selectResult.getByteBuffer().limit(size);
                        this.selectMappedBufferResult = selectResult;

                        //构建header
                        this.byteBufferHeader.position(0);
                        this.byteBufferHeader.limit(headerSize);
                        this.byteBufferHeader.putLong(thisOffset);
                        this.byteBufferHeader.putInt(size);
                        this.byteBufferHeader.flip();
                        //传输
                        this.lastWriteOver = this.transferData();
                    } else {

                        //如果没有新消息 写服务会阻塞等待100ms
                        HAConnection.this.haService.getWaitNotifyObject().allWaitForRunning(100);
                    }
                } catch (Exception e) {

                    HAConnection.log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            HAConnection.this.haService.getWaitNotifyObject().removeFromWaitingThreadTable();

            if (this.selectMappedBufferResult != null) {
                this.selectMappedBufferResult.release();
            }

            this.makeStop();

            readSocketService.makeStop();

            haService.removeConnection(HAConnection.this);

            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }

            HAConnection.log.info(this.getServiceName() + " service end");
        }

        /**
         * 传输master数据到slave
         * @return  true 本轮数据全部同步完成（header和数据块都同步完成）   false 本轮同步未完成（header和数据块有一个未同步完成就为未完成）
         * @throws Exception
         */
        private boolean transferData() throws Exception {
            int writeSizeZeroTimes = 0;
            // Write Header
            while (this.byteBufferHeader.hasRemaining()) {
                //byteBufferHeader中有待读取数据
                int writeSize = this.socketChannel.write(this.byteBufferHeader);
                if (writeSize > 0) {
                    //写成功
                    writeSizeZeroTimes = 0;
                    //更新写的时间戳
                    this.lastWriteTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                } else if (writeSize == 0) {
                    //超过三次没有数据写则退出循环
                    if (++writeSizeZeroTimes >= 3) {
                        break;
                    }
                } else {
                    throw new Exception("ha master write header error < 0");
                }
            }

            //本轮该传输给slave的数据
            if (null == this.selectMappedBufferResult) {
                //可能是个心跳包 只有header
                return !this.byteBufferHeader.hasRemaining();
            }

            writeSizeZeroTimes = 0;

            // 写数据块

            /**
             * !this.byteBufferHeader.hasRemaining() 成立表明 header发送成功
             */
            if (!this.byteBufferHeader.hasRemaining()) {
                //this.selectMappedBufferResult.getByteBuffer().hasRemaining() 成立表明有待传输消息数据
                while (this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
                    //写数据
                    int writeSize = this.socketChannel.write(this.selectMappedBufferResult.getByteBuffer());
                    if (writeSize > 0) {
                        //写成功
                        writeSizeZeroTimes = 0;
                        this.lastWriteTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                    } else if (writeSize == 0) {
                        //写失败 socket缓冲区写满了 尝试3次退出
                        if (++writeSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        throw new Exception("ha master write body error < 0");
                    }
                }
            }

            //header 和 body全部同步完成
            boolean result = !this.byteBufferHeader.hasRemaining() && !this.selectMappedBufferResult.getByteBuffer().hasRemaining();

            //表示本轮selectMappedBufferResult数据全部同步完成了
            if (!this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
                this.selectMappedBufferResult.release();
                this.selectMappedBufferResult = null;
            }

            return result;
        }

        @Override
        public String getServiceName() {
            return WriteSocketService.class.getSimpleName();
        }

        @Override
        public void shutdown() {
            super.shutdown();
        }
    }
}
