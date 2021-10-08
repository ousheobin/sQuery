package com.querytools.squery.io;

import com.querytools.squery.common.Config;
import com.querytools.squery.common.TableRegistry;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class QuickSelectUtil {

    private static final Logger logger = Logger.getLogger(QuickSelectUtil.class);
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
        index = Math.max(1, index);
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
        logger.debug("[Performance] read ("+table+
                ","+column+","+bucket+") in "+(selectStart - start)+" ms, select in "
                + (System.currentTimeMillis() - selectStart) + " ms");
        return res;
    }

    private long doSelect(int low, int high, int k) {
        int begin = low;
        int end = high;
        int mid = (low + high) / 2;
        long flag = data[mid];
        data[mid] = data[low];
        data[low] = flag;

        while (low < high){
            while (low < high && data[high] >= flag){
                high --;
            }
            if(low < high){
                data[low ++] = data[high];
            }
            while (low < high && data[low] < flag){
                low ++;
            }
            if(low < high){
                data[high --] = data[low];
            }
        }
        data[low] = flag;
        if(low == k - 1){
            return data[k-1];
        }else if(low < k - 1){
            return doSelect(low + 1, end, k );
        }else{
            return doSelect(begin, low - 1, k);
        }
    }

    public void loadData(File folder, int bucket, int count) throws IOException {
        if(data == null || data.length < count){
            logger.debug("Buffer size not enough. Increase to " + (count + 1));
            data = new long[count + 1];
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
            try{
                logger.debug("loading " + dataFile);
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
            }catch (Exception ex){
                logger.error("Error occurs when loading data file: " + dataFile + " channel:" + channel + " buf:" + buf, ex);
                throw ex;
            }
        }
    }

}
