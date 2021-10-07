package com.querytools.squery.io;

import com.querytools.squery.common.Config;
import com.querytools.squery.common.TableRegistry;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class QuickSelectUtil {

    private static final String CONCAT_SYMBOL = "$$";

    private TableRegistry registry = TableRegistry.getIntstance();
    private File workDir;

    private String bucketKey;
    private int count;
    private long[] data;
    private ByteBuffer buf;


    public QuickSelectUtil(File workDir){
        this.workDir = workDir;
    }

    public QuickSelectUtil(File workDir, int approximateBucketSize){
        this.workDir = workDir;
        this.data = new long[approximateBucketSize];
    }

    public long select(String table, String column, int bucket, int index) throws IOException {
        long start = System.currentTimeMillis();
        String key = table + CONCAT_SYMBOL + column + CONCAT_SYMBOL + bucket;
        if(!key.equals(bucketKey)){
            count = registry.getColumnBucketCount(table,column)[bucket];
            File folder = new File(workDir,table + "/" + column);
            this.loadData(folder,bucket, count);
            bucketKey = key;
        }
        long selectStart = System.currentTimeMillis();
        long res = doSelect(0, count - 1, index);
        System.out.println("[Performance]["+Thread.currentThread().getName()+"] read ("+table+
                ","+column+","+bucket+") \t in "+(selectStart - start)+" ms, select in "
                + (System.currentTimeMillis() - selectStart) + " ms");
        return res;
    }

    private long doSelect(int low, int high, int k) {
        int partition = partition(low, high);
        if (partition == k - 1){
            return data[partition];
        } else if (partition < k - 1){
            return doSelect(partition + 1, high, k);
        } else{
            return doSelect(low , partition - 1, k);
        }
    }

    private int partition(int left, int right) {
        long privot = data[right];
        int swapLoc = left;
        for (int i = left; i <= right - 1; i++) {
            if (data[i] <= privot) {
                long tmp = data[swapLoc];
                data[swapLoc] = data[i];
                data[i] = tmp;
                swapLoc++;
            }
        }
        long tmp = data[swapLoc];
        data[swapLoc] = data[right];
        data[right] = tmp;
        return swapLoc;
    }

    public void loadData(File folder, int bucket, int count) throws IOException {
        if(data == null || data.length < count){
            data = new long[count];
        }

        if(buf == null){
            buf = ByteBuffer.allocateDirect(Config.READ_BUFFER_SIZE);
        }

        File[] files = folder.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.startsWith(bucket + ".");
            }
        });

        int i = 0;
        for (File dataFile: files){
            RandomAccessFile raf = new RandomAccessFile(dataFile,"r");
            FileChannel channel = raf.getChannel();
            buf.clear();
            int len = channel.read(buf);
            if(len == -1){
                continue;
            }
            buf.flip();
            while (true){
                if(!buf.hasRemaining()){
                    buf.clear();
                    len = channel.read(buf);
                    if(len == -1){
                        break;
                    }
                    buf.flip();
                }
                data[i ++] = buf.getLong();
            }
            channel.close();
            raf.close();
        }
    }

}
