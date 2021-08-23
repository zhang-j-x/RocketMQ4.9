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

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * MappedFileQueue是 MappedFile的管理容器
 *对于Comitlog文件，每个文件1G，如果写满了，则新生成一个文件，MappedFile对象使用MappedFileQueue管理
 * 对于ConsumeQueue文件，每个topic有一个MappedFileQueue， 每个topic包含多个队列，一个队列一个MappedFile对象
 */
public class MappedFileQueue {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final InternalLogger LOG_ERROR = InternalLoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

    private static final int DELETE_FILES_BATCH_MAX = 10;

    /**存储目录(commitlog: ../store/commitlog 或者 consumeQueue: ../store/xxx_topic/0)*/
    private final String storePath;

    /**单个文件的存储大小(commitlog 默认1G consumeQueue: 默认600w字节) */
    private final int mappedFileSize;

    /**管理的MappedFile对象集合*/
    private final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<MappedFile>();

    /**创建MappedFile的服务类*/
    private final AllocateMappedFileService allocateMappedFileService;
    /**刷盘指针*/
    private long flushedWhere = 0;
    /**当前数据提交指针*/
    private long committedWhere = 0;

    /**当前目录最后一条消息存储时间戳*/
    private volatile long storeTimestamp = 0;

    public MappedFileQueue(final String storePath, int mappedFileSize,
        AllocateMappedFileService allocateMappedFileService) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.allocateMappedFileService = allocateMappedFileService;
    }

    public void checkSelf() {

        if (!this.mappedFiles.isEmpty()) {
            Iterator<MappedFile> iterator = mappedFiles.iterator();
            MappedFile pre = null;
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();

                if (pre != null) {
                    if (cur.getFileFromOffset() - pre.getFileFromOffset() != this.mappedFileSize) {
                        LOG_ERROR.error("[BUG]The mappedFile queue's data is damaged, the adjacent mappedFile's offset don't match. pre file {}, cur file {}",
                            pre.getFileName(), cur.getFileName());
                    }
                }
                pre = cur;
            }
        }
    }

    /**
     * 根据存储消息时间戳查找MappedFile
     * @param timestamp
     * @return
     */
    public MappedFile getMappedFileByTime(final long timestamp) {
        Object[] mfs = this.copyMappedFiles(0);

        if (null == mfs)
            return null;

        for (int i = 0; i < mfs.length; i++) {
            MappedFile mappedFile = (MappedFile) mfs[i];
            //从MappedFile列表的第一个文件开始找，找到第一个最后一次更新时间大于待查找时间戳的文件
            if (mappedFile.getLastModifiedTimestamp() >= timestamp) {
                return mappedFile;
            }
        }
            //如果不存在 则返回最后一个MappedFile文件
        return (MappedFile) mfs[mfs.length - 1];
    }

    private Object[] copyMappedFiles(final int reservedMappedFiles) {
        Object[] mfs;

        if (this.mappedFiles.size() <= reservedMappedFiles) {
            return null;
        }

        mfs = this.mappedFiles.toArray();
        return mfs;
    }

    public void truncateDirtyFiles(long offset) {
        List<MappedFile> willRemoveFiles = new ArrayList<MappedFile>();

        for (MappedFile file : this.mappedFiles) {
            long fileTailOffset = file.getFileFromOffset() + this.mappedFileSize;
            if (fileTailOffset > offset) {
                if (offset >= file.getFileFromOffset()) {
                    file.setWrotePosition((int) (offset % this.mappedFileSize));
                    file.setCommittedPosition((int) (offset % this.mappedFileSize));
                    file.setFlushedPosition((int) (offset % this.mappedFileSize));
                } else {
                    file.destroy(1000);
                    willRemoveFiles.add(file);
                }
            }
        }

        this.deleteExpiredFile(willRemoveFiles);
    }

    /**
     * 删除过期的文件
     * @param files
     */
    void deleteExpiredFile(List<MappedFile> files) {

        if (!files.isEmpty()) {

            Iterator<MappedFile> iterator = files.iterator();
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();
                if (!this.mappedFiles.contains(cur)) {
                    iterator.remove();
                    log.info("This mappedFile {} is not contained by mappedFiles, so skip it.", cur.getFileName());
                }
            }

            try {
                if (!this.mappedFiles.removeAll(files)) {
                    log.error("deleteExpiredFile remove failed.");
                }
            } catch (Exception e) {
                log.error("deleteExpiredFile has exception.", e);
            }
        }
    }

    /**
     * 加载storePath路径下的文件 放入 mappedFiles集合
     * @return
     */
    public boolean load() {
        File dir = new File(this.storePath);
        File[] files = dir.listFiles();
        if (files != null) {
            // ascending order
            Arrays.sort(files);
            for (File file : files) {

                if (file.length() != this.mappedFileSize) {
                    log.warn(file + "\t" + file.length()
                        + " length not matched message store config value, please check it manually");
                    return false;
                }

                try {
                    //创建对应的MappedFile对象
                    MappedFile mappedFile = new MappedFile(file.getPath(), mappedFileSize);
                    //此处写指针和刷盘指针都是设置的mappedFileSize，并不是最终值，准确值要在recover阶段设置
                    mappedFile.setWrotePosition(this.mappedFileSize);
                    mappedFile.setFlushedPosition(this.mappedFileSize);
                    mappedFile.setCommittedPosition(this.mappedFileSize);
                    //加入到集合
                    this.mappedFiles.add(mappedFile);
                    log.info("load " + file.getPath() + " OK");
                } catch (IOException e) {
                    log.error("load file " + file + " error", e);
                    return false;
                }
            }
        }

        return true;
    }

    public long howMuchFallBehind() {
        if (this.mappedFiles.isEmpty())
            return 0;

        long committed = this.flushedWhere;
        if (committed != 0) {
            MappedFile mappedFile = this.getLastMappedFile(0, false);
            if (mappedFile != null) {
                return (mappedFile.getFileFromOffset() + mappedFile.getWrotePosition()) - committed;
            }
        }

        return 0;
    }

    /**
     *
     * @param startOffset 文件起始偏移量
     * @param needCreate 是否需要创建文件
     * @return
     */
    public MappedFile getLastMappedFile(final long startOffset, boolean needCreate) {
        /**
         * 该值控制是否需要创建 mappedFile 当需要创建mappedFile时，它充当文件名的结尾
         * 两种情况会创建：
         *            (1) list中没有MappedFile
         *            (2) list最后一个MappedFile写满了
         */
        long createOffset = -1;
        MappedFile mappedFileLast = getLastMappedFile();

        if (mappedFileLast == null) {
            //情况(1) list中没有MappedFile   createOffset取值必须是mappedFileSize的倍数或者是0
            createOffset = startOffset - (startOffset % this.mappedFileSize);
        }

        if (mappedFileLast != null && mappedFileLast.isFull()) {
            //情况(2) list最后一个MappedFile写满了  上一个文件名转long + mappedFileSize
            createOffset = mappedFileLast.getFileFromOffset() + this.mappedFileSize;
        }

        //创建新的mappedFile逻辑
        if (createOffset != -1 && needCreate) {
            //获取率待创建文件的绝对路径（下次即将要创建的文件名）
            String nextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset);
            //获取下下次要创建的文件的绝对路径
            String nextNextFilePath = this.storePath + File.separator
                + UtilAll.offset2FileName(createOffset + this.mappedFileSize);
            MappedFile mappedFile = null;

            //allocateMappedFileService 创建MappedFile的服务 内部有自己的线程 向其提交请求 内部线程处理完后会返回结果
            //当mappedFileSize >= 1g 的话，allocateMappedFileService创建MappedFile会执行它的预热方法
            if (this.allocateMappedFileService != null) {
                mappedFile = this.allocateMappedFileService.putRequestAndReturnMappedFile(nextFilePath,
                    nextNextFilePath, this.mappedFileSize);
            } else {
                try {
                    mappedFile = new MappedFile(nextFilePath, this.mappedFileSize);
                } catch (IOException e) {
                    log.error("create mappedFile exception", e);
                }
            }

            if (mappedFile != null) {
                if (this.mappedFiles.isEmpty()) {
                    mappedFile.setFirstCreateInQueue(true);
                }
                this.mappedFiles.add(mappedFile);
            }

            return mappedFile;
        }

        return mappedFileLast;
    }

    public MappedFile getLastMappedFile(final long startOffset) {
        return getLastMappedFile(startOffset, true);
    }

    /**
     * 拿list中最后一个MappedFile
     * @return
     */
    public MappedFile getLastMappedFile() {
        MappedFile mappedFileLast = null;

        while (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileLast = this.mappedFiles.get(this.mappedFiles.size() - 1);
                break;
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getLastMappedFile has exception.", e);
                break;
            }
        }

        return mappedFileLast;
    }

    public boolean resetOffset(long offset) {
        MappedFile mappedFileLast = getLastMappedFile();

        if (mappedFileLast != null) {
            long lastOffset = mappedFileLast.getFileFromOffset() +
                mappedFileLast.getWrotePosition();
            long diff = lastOffset - offset;

            final int maxDiff = this.mappedFileSize * 2;
            if (diff > maxDiff)
                return false;
        }

        ListIterator<MappedFile> iterator = this.mappedFiles.listIterator();

        while (iterator.hasPrevious()) {
            mappedFileLast = iterator.previous();
            if (offset >= mappedFileLast.getFileFromOffset()) {
                int where = (int) (offset % mappedFileLast.getFileSize());
                mappedFileLast.setFlushedPosition(where);
                mappedFileLast.setWrotePosition(where);
                mappedFileLast.setCommittedPosition(where);
                break;
            } else {
                iterator.remove();
            }
        }
        return true;
    }

    public long getMinOffset() {

        if (!this.mappedFiles.isEmpty()) {
            try {
                return this.mappedFiles.get(0).getFileFromOffset();
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getMinOffset has exception.", e);
            }
        }
        return -1;
    }

    public long getMaxOffset() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getReadPosition();
        }
        return 0;
    }

    public long getMaxWrotePosition() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
        }
        return 0;
    }

    public long remainHowManyDataToCommit() {
        return getMaxWrotePosition() - committedWhere;
    }

    public long remainHowManyDataToFlush() {
        return getMaxOffset() - flushedWhere;
    }

    public void deleteLastMappedFile() {
        MappedFile lastMappedFile = getLastMappedFile();
        if (lastMappedFile != null) {
            lastMappedFile.destroy(1000);
            this.mappedFiles.remove(lastMappedFile);
            log.info("on recover, destroy a logic mapped file " + lastMappedFile.getFileName());

        }
    }

    /**
     * 删除commitlog过期文件 由定时任务调用
     * @param expiredTime  过期时间
     * @param deleteFilesInterval 删除两个文件的时间间隔
     * @param intervalForcibly 第一次尝试删除拒绝后文件能保留的最大时间
     * @param cleanImmediately true 强制删除 不考虑过期时间
     * @return
     */
    public int deleteExpiredFileByTime(final long expiredTime,
        final int deleteFilesInterval,
        final long intervalForcibly,
        final boolean cleanImmediately) {
        Object[] mfs = this.copyMappedFiles(0);

        if (null == mfs)
            return 0;
        //文件数组长度
        int mfsLength = mfs.length - 1;
        //删除文件数
        int deleteCount = 0;
        //删除文件列表
        List<MappedFile> files = new ArrayList<MappedFile>();
        if (null != mfs) {
            for (int i = 0; i < mfsLength; i++) {
                MappedFile mappedFile = (MappedFile) mfs[i];
                //文件存活时间点
                long liveMaxTimestamp = mappedFile.getLastModifiedTimestamp() + expiredTime;
                /**
                 * 条件1 ： 文件存活时间达到上限
                 * 条件2：磁盘占用率达到上限时会走强制删除
                 */
                if (System.currentTimeMillis() >= liveMaxTimestamp || cleanImmediately) {
                    if (mappedFile.destroy(intervalForcibly)) {
                        files.add(mappedFile);
                        deleteCount++;

                        //最多删除10个文件
                        if (files.size() >= DELETE_FILES_BATCH_MAX) {
                            break;
                        }
                        //如果删除的不是第一个文件 每删除一个文件都要sleep一下
                        if (deleteFilesInterval > 0 && (i + 1) < mfsLength) {
                            try {
                                Thread.sleep(deleteFilesInterval);
                            } catch (InterruptedException e) {
                            }
                        }
                    } else {
                        break;
                    }
                } else {
                    //avoid deleting files in the middle
                    break;
                }
            }
        }
        //从MappedFileQueue中删除
        deleteExpiredFile(files);

        return deleteCount;
    }

    /**
     * consumeQueue目录删除过期文件调用
     * @param offset commitlog目录下的最小偏移量
     * @param unitSize consumeQueue文件每个数据单元的固定大小
     * @return
     */
    public int deleteExpiredFileByOffset(long offset, int unitSize) {
        Object[] mfs = this.copyMappedFiles(0);

        List<MappedFile> files = new ArrayList<MappedFile>();
        int deleteCount = 0;
        if (null != mfs) {
            //保证当前正在顺序写的文件不会被删除
            int mfsLength = mfs.length - 1;

            for (int i = 0; i < mfsLength; i++) {
                boolean destroy;
                MappedFile mappedFile = (MappedFile) mfs[i];
                //获取当前文件最后一个数据单元
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer(this.mappedFileSize - unitSize);
                if (result != null) {
                    //获取数据偏移量
                    long maxOffsetInLogicQueue = result.getByteBuffer().getLong();
                    result.release();
                    //说明当前文件的所有数据都是过期数据
                    destroy = maxOffsetInLogicQueue < offset;
                    if (destroy) {
                        log.info("physic min offset " + offset + ", logics in current mappedFile max offset "
                            + maxOffsetInLogicQueue + ", delete it");
                    }
                } else if (!mappedFile.isAvailable()) { // Handle hanged file.
                    log.warn("Found a hanged consume queue file, attempting to delete it.");
                    destroy = true;
                } else {
                    log.warn("this being not executed forever.");
                    break;
                }

                if (destroy && mappedFile.destroy(1000 * 60)) {
                    files.add(mappedFile);
                    deleteCount++;
                } else {
                    break;
                }
            }
        }

        deleteExpiredFile(files);

        return deleteCount;
    }

    /**
     *
     * @param flushLeastPages flushLeastPages = 0 强制刷盘 flushLeastPages>0 脏页数量大于flushLeastPages 刷盘
     * @return true 本次刷盘无数据落盘 false 本次刷盘有数据落盘
     */
    public boolean flush(final int flushLeastPages) {
        boolean result = true;
        //获取当前正在刷盘的文件 （正在顺序写的MappedFile）
        MappedFile mappedFile = this.findMappedFileByOffset(this.flushedWhere, this.flushedWhere == 0);
        if (mappedFile != null) {
            //获取最后一条消息的存储时间
            long tmpTimeStamp = mappedFile.getStoreTimestamp();
            //刷盘 返回最新刷盘指针
            int offset = mappedFile.flush(flushLeastPages);
            //MappedFile起始偏移量 + 刷盘指针
            long where = mappedFile.getFileFromOffset() + offset;
            result = where == this.flushedWhere;
            this.flushedWhere = where;
            if (0 == flushLeastPages) {
                this.storeTimestamp = tmpTimeStamp;
            }
        }

        return result;
    }

    /**
     * 提交消息
     * 异步刷盘 并且启用TransientStorePool时  将堆外内存缓冲buffer的消息提交到MappedFile的fileChannel中
     * @param commitLeastPages
     * @return
     */
    public boolean commit(final int commitLeastPages) {
        boolean result = true;
        //找到提交指针所在MappedFile
        MappedFile mappedFile = this.findMappedFileByOffset(this.committedWhere, this.committedWhere == 0);
        if (mappedFile != null) {
            //调用MappedFile的commit方法提交消息
            int offset = mappedFile.commit(commitLeastPages);
            long where = mappedFile.getFileFromOffset() + offset;
            result = where == this.committedWhere;
            this.committedWhere = where;
        }

        return result;
    }

    /**
     * 根据offset查找MappedFile
     * @param offset
     * @param returnFirstOnNotFound 如果没有找到是否返回第一个MappedFile
     * @return
     */
    public MappedFile findMappedFileByOffset(final long offset, final boolean returnFirstOnNotFound) {
        try {
            //第一个MappedFile
            MappedFile firstMappedFile = this.getFirstMappedFile();
            //最后一个MappedFile
            MappedFile lastMappedFile = this.getLastMappedFile();
            if (firstMappedFile != null && lastMappedFile != null) {
                if (offset < firstMappedFile.getFileFromOffset() || offset >= lastMappedFile.getFileFromOffset() + this.mappedFileSize) {
                    //说明offset  没有命中list内的 MappedFile
                    LOG_ERROR.warn("Offset not matched. Request offset: {}, firstOffset: {}, lastOffset: {}, mappedFileSize: {}, mappedFiles count: {}",
                        offset,
                        firstMappedFile.getFileFromOffset(),
                        lastMappedFile.getFileFromOffset() + this.mappedFileSize,
                        this.mappedFileSize,
                        this.mappedFiles.size());
                } else {
                    //因为文件会删除 第一个文件的偏移地址不是0，所以不能通过offset / this.mappedFileSize来算文件号  要减去第一个文件的偏移地址
                    int index = (int) ((offset / this.mappedFileSize) - (firstMappedFile.getFileFromOffset() / this.mappedFileSize));
                    MappedFile targetFile = null;
                    try {
                        //获取MappedFile
                        targetFile = this.mappedFiles.get(index);
                    } catch (Exception ignored) {
                    }
                    //判定是否包含offset
                    if (targetFile != null && offset >= targetFile.getFileFromOffset()
                        && offset < targetFile.getFileFromOffset() + this.mappedFileSize) {
                        return targetFile;
                    }

                    for (MappedFile tmpMappedFile : this.mappedFiles) {
                        if (offset >= tmpMappedFile.getFileFromOffset()
                            && offset < tmpMappedFile.getFileFromOffset() + this.mappedFileSize) {
                            return tmpMappedFile;
                        }
                    }
                }

                if (returnFirstOnNotFound) {
                    return firstMappedFile;
                }
            }
        } catch (Exception e) {
            log.error("findMappedFileByOffset Exception", e);
        }

        return null;
    }

    public MappedFile getFirstMappedFile() {
        MappedFile mappedFileFirst = null;

        if (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileFirst = this.mappedFiles.get(0);
            } catch (IndexOutOfBoundsException e) {
                //ignore
            } catch (Exception e) {
                log.error("getFirstMappedFile has exception.", e);
            }
        }

        return mappedFileFirst;
    }

    /**
     * 根据消息的偏移量offset查找MappedFile
     * @param offset
     * @return
     */
    public MappedFile findMappedFileByOffset(final long offset) {
        return findMappedFileByOffset(offset, false);
    }

    public long getMappedMemorySize() {
        long size = 0;

        Object[] mfs = this.copyMappedFiles(0);
        if (mfs != null) {
            for (Object mf : mfs) {
                if (((ReferenceResource) mf).isAvailable()) {
                    size += this.mappedFileSize;
                }
            }
        }

        return size;
    }

    /**
     * 重试删除 删除应该删除但是未删除成功的文件
     * @param intervalForcibly
     * @return
     */
    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        //获取第一个MappedFile
        MappedFile mappedFile = this.getFirstMappedFile();
        if (mappedFile != null) {
            if (!mappedFile.isAvailable()) {
                //如果mappedFile不可用 则证明删除过 但是任然存在
                log.warn("the mappedFile was destroyed once, but still alive, " + mappedFile.getFileName());
                //调用mappedFile#destroy 销毁文件 如果120s后尝试关闭MappedFile 引用计数还是不为0  则每执行一次 引用计数减1000，直到释放资源
                boolean result = mappedFile.destroy(intervalForcibly);
                if (result) {
                    log.info("the mappedFile re delete OK, " + mappedFile.getFileName());
                    List<MappedFile> tmpFiles = new ArrayList<MappedFile>();
                    tmpFiles.add(mappedFile);
                    //从内存中删除
                    this.deleteExpiredFile(tmpFiles);
                } else {
                    log.warn("the mappedFile re delete failed, " + mappedFile.getFileName());
                }

                return result;
            }
        }

        return false;
    }

    public void shutdown(final long intervalForcibly) {
        for (MappedFile mf : this.mappedFiles) {
            mf.shutdown(intervalForcibly);
        }
    }

    public void destroy() {
        for (MappedFile mf : this.mappedFiles) {
            mf.destroy(1000 * 3);
        }
        this.mappedFiles.clear();
        this.flushedWhere = 0;

        // delete parent directory
        File file = new File(storePath);
        if (file.isDirectory()) {
            file.delete();
        }
    }

    public long getFlushedWhere() {
        return flushedWhere;
    }

    public void setFlushedWhere(long flushedWhere) {
        this.flushedWhere = flushedWhere;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public List<MappedFile> getMappedFiles() {
        return mappedFiles;
    }

    public int getMappedFileSize() {
        return mappedFileSize;
    }

    public long getCommittedWhere() {
        return committedWhere;
    }

    public void setCommittedWhere(final long committedWhere) {
        this.committedWhere = committedWhere;
    }
}
