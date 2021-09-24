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
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.PutMessageStatus;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 主从同步实现类  DefaultMessageStor构造方法中创建实例，DefaultMessageStor实例启动时启动HAService
 */
public class HAService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**当前主节点有多少个从节点与其进行数据同步*/
    private final AtomicInteger connectionCount = new AtomicInteger(0);
    /**为主节点和从节点的SocketChannel连接创建的HAConnection包装类集合*/
    private final List<HAConnection> connectionList = new LinkedList<>();
    /**master启动后后绑定服务器指定端口，监听slave的连接，AcceptSocketService封装了这些逻辑，此处使用的原生的NIO*/
    private final AcceptSocketService acceptSocketService;
    /**消息存储模块*/
    private final DefaultMessageStore defaultMessageStore;
    /**线程通信对象*/
    private final WaitNotifyObject waitNotifyObject = new WaitNotifyObject();
    /**master向slave节点推送的最大的消息偏移量 如果有多个slave，则是同步最快的那个slave的同步进度*/
    private final AtomicLong push2SlaveMaxOffset = new AtomicLong(0);
    /**控制生产者线程阻塞等待的逻辑*/
    private final GroupTransferService groupTransferService;

    /**slave节点的客户端对象*/
    private final HAClient haClient;

    public HAService(final DefaultMessageStore defaultMessageStore) throws IOException {
        this.defaultMessageStore = defaultMessageStore;
        this.acceptSocketService =
            new AcceptSocketService(defaultMessageStore.getMessageStoreConfig().getHaListenPort());
        this.groupTransferService = new GroupTransferService();
        this.haClient = new HAClient();
    }

    public void updateMasterAddress(final String newAddr) {
        if (this.haClient != null) {
            this.haClient.updateMasterAddress(newAddr);
        }
    }

    public void putRequest(final CommitLog.GroupCommitRequest request) {
        this.groupTransferService.putRequest(request);
    }

    /**
     * 判断从节点是否可用
     *  1、主从连接建立
     *  2、主节点的消息最大偏移量 和 最大的从节点同步进度差值大于配置阈值 256M
     * @param masterPutWhere
     * @return
     */
    public boolean isSlaveOK(final long masterPutWhere) {
        boolean result = this.connectionCount.get() > 0;
        result =
            result
                && ((masterPutWhere - this.push2SlaveMaxOffset.get()) < this.defaultMessageStore
                .getMessageStoreConfig().getHaSlaveFallbehindMax());
        return result;
    }

    /**
     * 更新最大的同步进度
     * @param offset
     */
    public void notifyTransferSome(final long offset) {
        //更新slave端最大同步进度（如果是多个slave就是同步进度最快的那个）
        for (long value = this.push2SlaveMaxOffset.get(); offset > value; ) {
            boolean ok = this.push2SlaveMaxOffset.compareAndSet(value, offset);
            if (ok) {
                this.groupTransferService.notifyTransferSome();
                break;
            } else {
                value = this.push2SlaveMaxOffset.get();
            }
        }
    }

    public AtomicInteger getConnectionCount() {
        return connectionCount;
    }

    // public void notifyTransferSome() {
    // this.groupTransferService.notifyTransferSome();
    // }

    public void start() throws Exception {
        this.acceptSocketService.beginAccept();
        this.acceptSocketService.start();
        this.groupTransferService.start();
        this.haClient.start();
    }

    public void addConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.add(conn);
        }
    }

    public void removeConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.remove(conn);
        }
    }

    public void shutdown() {
        this.haClient.shutdown();
        this.acceptSocketService.shutdown(true);
        this.destroyConnections();
        this.groupTransferService.shutdown();
    }

    public void destroyConnections() {
        synchronized (this.connectionList) {
            for (HAConnection c : this.connectionList) {
                c.shutdown();
            }

            this.connectionList.clear();
        }
    }

    public DefaultMessageStore getDefaultMessageStore() {
        return defaultMessageStore;
    }

    public WaitNotifyObject getWaitNotifyObject() {
        return waitNotifyObject;
    }

    public AtomicLong getPush2SlaveMaxOffset() {
        return push2SlaveMaxOffset;
    }

    /**
     * Listens to slave connections to create {@link HAConnection}.
     */
    class AcceptSocketService extends ServiceThread {
        /**mater 绑定的HA监听端口*/
        private final SocketAddress socketAddressListen;
        /**mater 连接通道*/
        private ServerSocketChannel serverSocketChannel;
        /**选择器*/
        private Selector selector;

        /**
         * 默认端口10912
         * @param port
         */
        public AcceptSocketService(final int port) {
            this.socketAddressListen = new InetSocketAddress(port);
        }

        /**
         * Starts listening to slave connections.
         *
         * @throws Exception If fails.
         */
        public void beginAccept() throws Exception {
            //获取服务端 serverSocketChannel
            this.serverSocketChannel = ServerSocketChannel.open();
            //初始化选择器
            this.selector = RemotingUtil.openSelector();
            this.serverSocketChannel.socket().setReuseAddress(true);
            //绑定ha的监听端口  默认端口10912
            this.serverSocketChannel.socket().bind(this.socketAddressListen);
            //设置非阻塞
            this.serverSocketChannel.configureBlocking(false);
            //注册选择器并监听连接事件
            this.serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void shutdown(final boolean interrupt) {
            super.shutdown(interrupt);
            try {
                this.serverSocketChannel.close();
                this.selector.close();
            } catch (IOException e) {
                log.error("AcceptSocketService shutdown exception", e);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    //选择器阻塞1s
                    this.selector.select(1000);
                    /**
                     * 执行到这里有哪些情况？
                     * 1、通道中有连接事件就绪
                     * 2、选择器select方法超时
                     */
                    Set<SelectionKey> selected = this.selector.selectedKeys();

                    if (selected != null) {
                        for (SelectionKey k : selected) {
                            if ((k.readyOps() & SelectionKey.OP_ACCEPT) != 0) {
                                //获取客户端连接
                                SocketChannel sc = ((ServerSocketChannel) k.channel()).accept();

                                if (sc != null) {
                                    HAService.log.info("HAService receive new connection, "
                                        + sc.socket().getRemoteSocketAddress());

                                    try {
                                        //封装成HAConnection对象
                                        HAConnection conn = new HAConnection(HAService.this, sc);
                                        /**
                                         * 启动HAConnection对象，启动内部的两个服务
                                         *      1、读数据服务
                                         *      2、写数据服务
                                         *
                                         */
                                        conn.start();
                                        //HAConnection加入到connectionList集合中
                                        HAService.this.addConnection(conn);
                                    } catch (Exception e) {
                                        log.error("new HAConnection exception", e);
                                        sc.close();
                                    }
                                }
                            } else {
                                log.warn("Unexpected ops in select " + k.readyOps());
                            }
                        }

                        selected.clear();
                    }
                } catch (Exception e) {
                    log.error(this.getServiceName() + " service has exception.", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String getServiceName() {
            return AcceptSocketService.class.getSimpleName();
        }
    }

    /**
     * 主从同步服务
     */
    class GroupTransferService extends ServiceThread {

        private final WaitNotifyObject notifyTransferObject = new WaitNotifyObject();
        /**写队列 存放存储模块写消息后主从同步的请求*/
        private volatile List<CommitLog.GroupCommitRequest> requestsWrite = new ArrayList<>();
        /**读队列 用于主从同步*/
        private volatile List<CommitLog.GroupCommitRequest> requestsRead = new ArrayList<>();

        public synchronized void putRequest(final CommitLog.GroupCommitRequest request) {
            synchronized (this.requestsWrite) {
                this.requestsWrite.add(request);
            }
            this.wakeup();
        }

        /**
         * 当HAConnection的读服务接收到了slave返回就master的同步进度时，调用该方法
         */
        public void notifyTransferSome() {
            this.notifyTransferObject.wakeup();
        }

        private void swapRequests() {
            List<CommitLog.GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        }

        /**
         * 等待消息同步
         */
        private void doWaitTransfer() {
            synchronized (this.requestsRead) {
                if (!this.requestsRead.isEmpty()) {
                    for (CommitLog.GroupCommitRequest req : this.requestsRead) {
                        //判断slave节点最大同步进度 是否已经超过消息的偏移量，大于则证明消息已经同步到从库
                        boolean transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                        //等待超时时间
                        long waitUntilWhen = HAService.this.defaultMessageStore.getSystemClock().now()
                            + HAService.this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout();
                        //如果没有同步成功
                        while (!transferOK && HAService.this.defaultMessageStore.getSystemClock().now() < waitUntilWhen) {
                            //GroupTransferService线程再次阻塞，slave端向master返回同步进度后，会将其唤醒
                            this.notifyTransferObject.waitForRunning(1000);
                            transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                        }

                        if (!transferOK) {
                            log.warn("transfer messsage to slave timeout, " + req.getNextOffset());
                        }

                        //唤醒阻塞的消息发送线程
                        req.wakeupCustomer(transferOK ? PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_SLAVE_TIMEOUT);
                    }

                    this.requestsRead.clear();
                }
            }
        }

        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.waitForRunning(10);
                    this.doWaitTransfer();
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            return GroupTransferService.class.getSimpleName();
        }
    }

    /**
     * slave端运行的HA客户端代码，它会和master服务器建立长连接，上报本地的同步进度，同步master发送的消息数据
     */
    class HAClient extends ServiceThread {
        /**读缓冲区大小 4Mb*/
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024 * 4;
        /**master节点启动时监听的HA会话端口*/
        private final AtomicReference<String> masterAddress = new AtomicReference<>();
        /**slave向master发起主从同步的拉取偏移量*/
        private final ByteBuffer reportOffset = ByteBuffer.allocate(8);
        /**网络传输通道*/
        private SocketChannel socketChannel;
        /**nio事件选择器*/
        private Selector selector;
        /**上一次写入时间戳*/
        private long lastWriteTimestamp = System.currentTimeMillis();
        /**slave当前的复制进度，commitlog文件的最大偏移量*/
        private long currentReportedOffset = 0;
        /**本次已处理读缓冲区的指针*/
        private int dispatchPosition = 0;
        /**
         *读缓冲区 大小为4M 用于socket缓冲区加载就绪的数据
         *
         * 最后一条数据大概率是一条半包数据，此时会将半包数据写到byteBufferBackup，然后byteBufferRead重置，byteBufferRead和byteBufferBackup交换
         *
         *  master与slave的数据传输格式
         *  {[phyOffset][size][data][phyOffset][size][data][phyOffset][size][data]}
         *  phyOffset：数据区间开始的偏移量，并不表示某条具体的数据消息，表示数据块开始偏移量位置
         *  size: 同步的数据块大小
         *  data: 数据块 最大32kb 可能包含多条数据消息
         *
         * */
        private ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
        /**读缓冲区备份，与byteBufferRead进行交换*/
        private ByteBuffer byteBufferBackup = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);

        public HAClient() throws IOException {
            this.selector = RemotingUtil.openSelector();
        }

        /**
         * slave角色的节点会调用该方法，传递master节点暴露的ha地址端口信息
         * master节点是不会调用的，master节点的masterAddress为空
         * @param newAddr
         */
        public void updateMasterAddress(final String newAddr) {
            String currentAddr = this.masterAddress.get();
            if (currentAddr == null || !currentAddr.equals(newAddr)) {
                this.masterAddress.set(newAddr);
                log.info("update master address, OLD: " + currentAddr + " NEW: " + newAddr);
            }
        }

        private boolean isTimeToReportOffset() {
            long interval =
                HAService.this.defaultMessageStore.getSystemClock().now() - this.lastWriteTimestamp;
            boolean needHeart = interval > HAService.this.defaultMessageStore.getMessageStoreConfig()
                .getHaSendHeartbeatInterval();

            return needHeart;
        }

        private boolean reportSlaveMaxOffset(final long maxOffset) {
            this.reportOffset.position(0);
            this.reportOffset.limit(8);
            this.reportOffset.putLong(maxOffset);
            this.reportOffset.position(0);
            this.reportOffset.limit(8);

            for (int i = 0; i < 3 && this.reportOffset.hasRemaining(); i++) {
                try {
                    this.socketChannel.write(this.reportOffset);
                } catch (IOException e) {
                    log.error(this.getServiceName()
                        + "reportSlaveMaxOffset this.socketChannel.write exception", e);
                    return false;
                }
            }

            lastWriteTimestamp = HAService.this.defaultMessageStore.getSystemClock().now();
            //写成功 pos == limit
            return !this.reportOffset.hasRemaining();
        }

        /**
         * 处理byteBufferRead写满的情况
         *  将byteBufferRead的半包数据写入到byteBufferBackup，byteBufferRead 和 byteBufferBackup交换
         */
        private void reallocateByteBuffer() {
            //remain尚未处理过的字节数量
            int remain = READ_MAX_BUFFER_SIZE - this.dispatchPosition;
            //存在半包数据
            if (remain > 0) {
                this.byteBufferRead.position(this.dispatchPosition);

                //将半包数据写到byteBufferBackup中
                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);
                this.byteBufferBackup.put(this.byteBufferRead);
            }

            //byteBufferRead 和 byteBufferBackup交换
            this.swapByteBuffer();

            //设置 新byteBufferRead的写指针为remain ，因为从旧的byteBufferRead将remain长度的半包数据写入了
            this.byteBufferRead.position(remain);
            this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            this.dispatchPosition = 0;
        }

        private void swapByteBuffer() {
            ByteBuffer tmp = this.byteBufferRead;
            this.byteBufferRead = this.byteBufferBackup;
            this.byteBufferBackup = tmp;
        }

        /**
         * 处理master发送给slave数据的逻辑
         * @return  true 处理成功 false socket处于半关闭状态
         */
        private boolean processReadEvent() {
            //控制while循环的变量 值为3时跳出循环
            int readSizeZeroTimes = 0;
            //byteBufferRead还有剩余空间去socket读缓冲区加载数据
            while (this.byteBufferRead.hasRemaining()) {
                try {
                    //读取数据到byteBufferRead
                    int readSize = this.socketChannel.read(this.byteBufferRead);


                    if (readSize > 0) {
                        //读取到了数据
                        readSizeZeroTimes = 0;
                        //处理读取到的master发给slave的数据
                        boolean result = this.dispatchReadRequest();
                        if (!result) {
                            log.error("HAClient, dispatchReadRequest error");
                            return false;
                        }
                    } else if (readSize == 0) {
                        //无新数据
                        if (++readSizeZeroTimes >= 3) {
                            //正常都是从这里跳出循环
                            break;
                        }
                    } else {
                        //readSize = -1,对端关闭了连接
                        log.info("HAClient, processReadEvent read socket < 0");
                        return false;
                    }
                } catch (IOException e) {
                    log.info("HAClient, processReadEvent read socket exception", e);
                    return false;
                }
            }

            return true;
        }

        /**
         * 处理读取到的master发给slave的数据
         * @return
         */
        private boolean dispatchReadRequest() {
            /**
             * master与slave的数据传输格式 {[phyOffset][size][data][phyOffset][size][data][phyOffset][size][data]}
             */
            final int msgHeaderSize = 8 + 4; // phyoffset + size
            //读取byteBufferRead处理数据之前的position,用于处理完数据后恢复position指针
            int readSocketPos = this.byteBufferRead.position();

            while (true) {
                //byteBufferRead中未处理的数据量，每处理一条数据都会更新dispatchPosition
                int diff = this.byteBufferRead.position() - this.dispatchPosition;

                //byteBufferRead中最起码有完整的header数据 [phyOffset][size]
                if (diff >= msgHeaderSize) {
                    //获取master数据块的物理偏移量
                    long masterPhyOffset = this.byteBufferRead.getLong(this.dispatchPosition);
                    //数据块大小
                    int bodySize = this.byteBufferRead.getInt(this.dispatchPosition + 8);

                    //获取slave的最大消息物理偏移量
                    long slavePhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();


                    /**
                     * 正常情况下 masterPhyOffset和slavePhyOffset是相等的
                     *  slavePhyOffset是slave端的最大消息物理偏移量，masterPhyOffset是目前正在同步的master消息偏移量
                     */
                    if (slavePhyOffset != 0) {
                        if (slavePhyOffset != masterPhyOffset) {
                            log.error("master pushed offset not equal the max phy offset in slave, SLAVE: "
                                + slavePhyOffset + " MASTER: " + masterPhyOffset);
                            return false;
                        }
                    }

                    //判断是否半包 返回true证明整个包是完整的
                    if (diff >= (msgHeaderSize + bodySize)) {
                        //创建一个字节数组 提取消息的数据库
                        byte[] bodyData = new byte[bodySize];
                        this.byteBufferRead.position(this.dispatchPosition + msgHeaderSize);
                        //读取消息数据块数据
                        this.byteBufferRead.get(bodyData);
                        //调用commitlog追加消息到commitlog文件中去
                        HAService.this.defaultMessageStore.appendToCommitLog(masterPhyOffset, bodyData);
                        //恢复到读指针
                        this.byteBufferRead.position(readSocketPos);
                        //增加dispatchPosition
                        this.dispatchPosition += msgHeaderSize + bodySize;

                        //上报slave的同步进度
                        if (!reportSlaveMaxOffsetPlus()) {
                            return false;
                        }

                        continue;
                    }
                }

                /**
                 *当diff >= msgHeaderSize条件不成立 或者 diff >= (msgHeaderSize + bodySize)条件不成立 就会执行到此处
                 *
                 *处理byteBufferRead写满的情况
                 *         将byteBufferRead的半包数据写入到byteBufferBackup，byteBufferRead 和 byteBufferBackup交换
                 */

                if (!this.byteBufferRead.hasRemaining()) {
                    this.reallocateByteBuffer();
                }

                break;
            }

            return true;
        }

        private boolean reportSlaveMaxOffsetPlus() {
            boolean result = true;
            long currentPhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();
            if (currentPhyOffset > this.currentReportedOffset) {
                this.currentReportedOffset = currentPhyOffset;
                result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                if (!result) {
                    this.closeMaster();
                    log.error("HAClient, reportSlaveMaxOffset error, " + this.currentReportedOffset);
                }
            }

            return result;
        }

        /**
         * 连接master服务器
         * @return
         * @throws ClosedChannelException
         */
        private boolean connectMaster() throws ClosedChannelException {
            //如果socketChannel为空则尝试连接master
            if (null == socketChannel) {
                //获取master的地址
                String addr = this.masterAddress.get();
                if (addr != null) {
                    //转换
                    SocketAddress socketAddress = RemotingUtil.string2SocketAddress(addr);
                    if (socketAddress != null) {
                        //连接mater对应的HA端口
                        this.socketChannel = RemotingUtil.connect(socketAddress);
                        if (this.socketChannel != null) {
                            //通道注册到选择器 并监听可读事件
                            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
                        }
                    }
                }
                //初始化slave当前的复制进度 为slave当前commitlog的最大偏移量
                this.currentReportedOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();
                //更新上一次写入时间戳
                this.lastWriteTimestamp = System.currentTimeMillis();
            }

            return this.socketChannel != null;
        }

        private void closeMaster() {
            if (null != this.socketChannel) {
                try {

                    SelectionKey sk = this.socketChannel.keyFor(this.selector);
                    if (sk != null) {
                        sk.cancel();
                    }

                    this.socketChannel.close();

                    this.socketChannel = null;
                } catch (IOException e) {
                    log.warn("closeMaster exception. ", e);
                }

                this.lastWriteTimestamp = 0;
                this.dispatchPosition = 0;

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);

                this.byteBufferRead.position(0);
                this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            }
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    //连接master服务器
                    if (this.connectMaster()) {

                        //slave每5秒钟一定会上报slave端的同步进度信息给master
                        if (this.isTimeToReportOffset()) {
                            //上报slave端消息进度
                            boolean result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                            if (!result) {
                                this.closeMaster();
                            }
                        }
                        //选择器阻塞
                        this.selector.select(1000);

                        /**
                         *执行到这里的情况
                         *  1、socketChannel中部OP_READ事件就绪
                         *  2、选择器select方法超时
                         */

                        //HAClient的核心：处理master发送给slave数据的逻辑
                        boolean ok = this.processReadEvent();
                        if (!ok) {
                            this.closeMaster();
                        }

                        if (!reportSlaveMaxOffsetPlus()) {
                            continue;
                        }

                        long interval =
                            HAService.this.getDefaultMessageStore().getSystemClock().now()
                                - this.lastWriteTimestamp;
                        if (interval > HAService.this.getDefaultMessageStore().getMessageStoreConfig()
                            .getHaHousekeepingInterval()) {
                            log.warn("HAClient, housekeeping, found this connection[" + this.masterAddress
                                + "] expired, " + interval);
                            this.closeMaster();
                            log.warn("HAClient, master not response some time, so close connection");
                        }
                    } else {
                        this.waitForRunning(1000 * 5);
                    }
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                    this.waitForRunning(1000 * 5);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        @Override
        public void shutdown() {
            super.shutdown();
            closeMaster();
        }

        // private void disableWriteFlag() {
        // if (this.socketChannel != null) {
        // SelectionKey sk = this.socketChannel.keyFor(this.selector);
        // if (sk != null) {
        // int ops = sk.interestOps();
        // ops &= ~SelectionKey.OP_WRITE;
        // sk.interestOps(ops);
        // }
        // }
        // }
        // private void enableWriteFlag() {
        // if (this.socketChannel != null) {
        // SelectionKey sk = this.socketChannel.keyFor(this.selector);
        // if (sk != null) {
        // int ops = sk.interestOps();
        // ops |= SelectionKey.OP_WRITE;
        // sk.interestOps(ops);
        // }
        // }
        // }

        @Override
        public String getServiceName() {
            return HAClient.class.getSimpleName();
        }
    }
}
