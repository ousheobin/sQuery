package com.querytools.squery.io;

import com.querytools.squery.common.Config;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class ReadThread implements Runnable {

    private static final long[] transformMap = new long[Byte.MAX_VALUE];

    static {
        for (int i = 0; i < 10 ; i ++ ){
            transformMap['0' + i] = i;
        }
    }

    private int threadIndex;

    private ReadController readController;
    private FileChannel channel;
    private ByteBuffer buffer;
    private BucketWriter[] writers;

    ReadThread(ReadController readController,int threadIndex){
        this.readController = readController;
        this.threadIndex = threadIndex;
    }

    @Override
    public void run() {
        int readCnt = 0;
        long[] bound = readController.pollTask();
        if(bound == null || bound[0] == -1){
            readController.singleTaskComplete();
            return;
        }
        try {
            RandomAccessFile raf = new RandomAccessFile(readController.getDataFile(),"r");

            channel = raf.getChannel();
            buffer = ByteBuffer.allocateDirect(Config.READ_BUFFER_SIZE);

            String[] columns = readController.getColumns();
            int columnCnt = columns.length;

            this.writers = new BucketWriter[columnCnt];
            int writerIndex = 0;
            for (File folder : readController.getColumnFolders()){
                writers[writerIndex ++] = new BucketWriter(folder, threadIndex);
            }

            long currpos = bound[0];
            long upperBound = bound[1];

            byte ch;
            long value = 0;
            int currColumn = 0;

            channel.position(currpos);

            int len = channel.read(buffer);
            buffer.flip();

            if(len == -1){
                readController.singleTaskComplete();
                return;
            }

            while (currpos < upperBound){
                if(!buffer.hasRemaining()){
                    buffer.clear();
                    len = channel.read(buffer);
                    if(len == -1){
                        break;
                    }
                    buffer.flip();
                }
                ch = buffer.get();
                if(ch >= '0' && ch <= '9'){
                    value = value * 10 + transformMap[ch];
                }else if(ch == ',' || ch == '\n'){
                    writers[currColumn].write(value);
                    value = 0;
                    currColumn = (currColumn + 1) % columnCnt;
                    if(ch == '\n'){
                        readCnt ++;
                    }
                }
                currpos ++;
            }

            channel.close();
            raf.close();

            for (BucketWriter writer: writers){
                writer.close();
            }

            for (int i = 0; i < columnCnt; i ++){
                readController.updateBucketCnt(columns[i],writers[i].getBucketCount());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        readController.updateReadCnt(readCnt);
        readController.singleTaskComplete();
    }

}
