package com.querytools.squery.io;

import com.querytools.squery.common.Config;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class ReadController {

    private static final Logger logger = Logger.getLogger(ReadController.class);

    private int subTaskCnt;

    private File dataFile;
    private File workSpace;
    private CountDownLatch latch;
    private AtomicInteger readCnt;

    private String[] columns;
    private File[] columnFolders;
    private Queue<long[]> task;

    private ConcurrentHashMap<String, AtomicInteger[]> bucketCount;

    ReadController(File dataFile, File workSpace ,int subTaskCnt){
        this.dataFile = dataFile;
        this.workSpace = workSpace;
        this.latch = new CountDownLatch(subTaskCnt);
        this.subTaskCnt = subTaskCnt;
        this.readCnt = new AtomicInteger(0);
        this.bucketCount = new ConcurrentHashMap<>();
        this.initColumns();
    }

    private void initColumns(){
        StringBuilder stringBuilder = new StringBuilder();
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(dataFile, "r");
            FileChannel channel = randomAccessFile.getChannel();
            ByteBuffer headerBuf = ByteBuffer.allocateDirect(Config.PREPARE_BUF_SIZE);
            long fileLength = randomAccessFile.length();
            long eachThreadRead = fileLength / Math.max(this.subTaskCnt,1);
            logger.debug("File size: " + fileLength + ", Each thread expect read: " + eachThreadRead);

            int size = channel.read(headerBuf);
            if(size == -1){
                throw new RuntimeException("Read Error! Size -1.");
            }
            headerBuf.flip();
            byte ch;
            while (headerBuf.hasRemaining()){
                ch = headerBuf.get();
                if(ch == '\n'){
                    break;
                }
                stringBuilder.append((char)ch);
            }

            columns = stringBuilder.toString().split(",");

            columnFolders = new File[columns.length];
            for (int i = 0 ; i < columns.length; i ++ ){
                columnFolders[i] = new File(workSpace, columns[i]);
                columnFolders[i].mkdirs();
                AtomicInteger[] counter = new AtomicInteger[Config.BUCKET_CNT];
                for (int j = 0 ; j < counter.length; j ++ ){
                    counter[j] = new AtomicInteger(0);
                }
                this.bucketCount.putIfAbsent(columns[i],counter);
            }

            long begin = headerBuf.position();
            long end = 0;

            this.task = new ConcurrentLinkedQueue<>();
            for (int i = 0 ; i < subTaskCnt ; i ++ ){
                end = Math.min(begin + eachThreadRead, fileLength);
                if( begin >= fileLength){
                    task.offer(new long[]{-1,-1});
                    continue;
                }
                long pos = Math.max(0, end - Config.PREPARE_BUF_SIZE);
                channel.position(pos);
                headerBuf.clear();
                int len = channel.read(headerBuf);
                if(i == subTaskCnt - 1){
                    end = fileLength;
                }else{
                    while (len > 0){
                        if(headerBuf.get(len - 1) == '\n'){
                            break;
                        }
                        len --;
                    }
                    end = pos + len;
                }
                task.offer(new long[]{begin,end});
                begin = end;
            }

            channel.close();
            randomAccessFile.close();

        } catch (IOException e) {
            logger.error("Failed to initial columns info", e);
        }
    }

    public File getDataFile() {
        return dataFile;
    }

    public File[] getColumnFolders() {
        return columnFolders;
    }

    public void singleTaskComplete(){
        latch.countDown();
    }

    public void waitAllTaskComplete(){
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Caught exception when waiting all task to be completed",e);
        }
    }

    public void updateReadCnt(int readCnt){
        this.readCnt.addAndGet(readCnt);
    }

    public int getTotalRead(){
        return readCnt.get();
    }

    public String[] getColumns() {
        return columns;
    }

    public long[] pollTask(){
        return task.poll();
    }

    public void updateBucketCnt(String column, int[] newVal){
        AtomicInteger[] ref = this.bucketCount.get(column);
        if(ref == null){
            return;
        }
        for (int i = 0 ; i < ref.length; i ++ ){
            ref[i].addAndGet(newVal[i]);
        }
    }

    public Map<String, int[]> getBucketCnt(){
        HashMap<String,int[]> ret = new HashMap<>();
        for (String key: this.bucketCount.keySet()){
            AtomicInteger[] oldArr = this.bucketCount.get(key);
            int newArr[] = new int[oldArr.length];
            for (int i = 0; i < oldArr.length; i ++ ){
                newArr[i] = oldArr[i].get();
            }
            ret.put(key,newArr);
        }
        return ret;
    }

}
