package com.querytools.squery.io;

import com.querytools.squery.common.Config;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class BucketWriter {

    private int bucketIndex;

    private File workdir;
    private RandomAccessFile rafs[];
    private FileChannel channels[];
    private ByteBuffer buffer[];
    private int bucketCount[];

    public BucketWriter(File workdir, int bucketIndex){
        this.workdir = workdir;
        this.bucketCount = new int[Config.BUCKET_CNT];
        this.rafs = new RandomAccessFile[Config.BUCKET_CNT];
        this.channels = new FileChannel[Config.BUCKET_CNT];
        this.buffer = new ByteBuffer[Config.BUCKET_CNT];
        this.bucketIndex = bucketIndex;
    }

    public void write(long value) {
        int bucket = (int)(value >> (64 - (Config.BUCKET_MASK_BITS + 1)));
        try{
            if(channels[bucket] == null){
                rafs[bucket] = new RandomAccessFile(new File(workdir, bucket +"."+bucketIndex+ ".data"),"rw");
                channels[bucket] = rafs[bucket].getChannel();
                buffer[bucket] = ByteBuffer.allocateDirect(Config.BUFFER_SIZE);
            }
            if(!buffer[bucket].hasRemaining()){
                buffer[bucket].flip();
                channels[bucket].write(buffer[bucket]);
                buffer[bucket].clear();
            }
            buffer[bucket].putLong(value);
            bucketCount[bucket] ++;
        }catch (Exception ex){
            ex.printStackTrace();
        }
    }

    public void close() throws IOException {
        for ( int bucket = 0 ; bucket < Config.BUCKET_CNT ; bucket ++ ){
            if(buffer[bucket] == null){
                continue;
            }
            if(buffer[bucket].position() > 0) {
                buffer[bucket].flip();
                channels[bucket].write(buffer[bucket]);
                buffer[bucket] = ByteBuffer.allocateDirect(Config.BUFFER_SIZE);
            }
            if(channels[bucket]!=null && rafs[bucket] !=null) {
                channels[bucket].close();
                rafs[bucket].close();
            }
        }
    }

    public int[] getBucketCount() {
        return bucketCount;
    }

}
