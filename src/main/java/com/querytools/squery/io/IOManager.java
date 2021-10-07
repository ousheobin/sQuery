package com.querytools.squery.io;

import com.querytools.squery.common.TableInfo;
import com.querytools.squery.common.TableRegistry;
import com.querytools.squery.common.ThreadPool;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class IOManager {

    private String workspaceDir;
    private TableRegistry tableRegistry = TableRegistry.getIntstance();
    private ThreadLocal<QuickSelectUtil> selectUtilThreadLocal;

    private int approximateMaxBucketCnt = 1;

    public IOManager(String workspaceDir) {
        this.workspaceDir = workspaceDir;
        File workspace = new File(workspaceDir);
        if(!workspace.exists()){
            throw new IllegalArgumentException("Dir not found: " + workspaceDir);
        }
        this.selectUtilThreadLocal = new ThreadLocal<>();
    }

    public void setApproximateMaxBucketCnt(int approximateMaxBucketCnt) {
        this.approximateMaxBucketCnt = approximateMaxBucketCnt;
    }

    public String load(File dataFile) throws Exception {
        File workspaceFolder = new File(workspaceDir);
        String tableName = dataFile.getName();
        if(tableName.contains(".")){
            tableName = tableName.substring(0, tableName.lastIndexOf('.'));
        }
        int threadCnt = Runtime.getRuntime().availableProcessors() * 2;
        ThreadPool threadPool = ThreadPool.getInstance();
        File dataDir = new File(workspaceFolder, tableName);
        System.out.println("[Info] Start loading table: " + tableName);
        long startTime = System.currentTimeMillis();
        ReadController readController = new ReadController(dataFile,dataDir, threadCnt);
        System.out.println("[Performance] Prepare used " + (System.currentTimeMillis() -startTime) + " ms");
        for (int i = 0 ; i < threadCnt ; i ++ ){
            threadPool.submit(new ReadThread(readController, i));
        }
        readController.waitAllTaskComplete();
        TableInfo info = new TableInfo();
        info.setTableName(tableName);
        info.setTableLineCnt(readController.getTotalRead());
        info.setBucketCnt(readController.getBucketCnt());
        tableRegistry.addTable(info);
        System.out.println("[Debug] Columns: " + Arrays.toString(readController.getColumns()));
        System.out.println("[Debug] Total load record: " + readController.getTotalRead());
        System.out.println("[Performance] Load data in "+ (System.currentTimeMillis() - startTime) + " ms.");
        return tableName;
    }

    public String readByIndex(String table, String column, int index) throws Exception {
        int counter[] = tableRegistry.getColumnBucketCount(table,column);
        if(counter == null){
            return "";
        }
        int total = 0;
        int bucket = 0;
        for ( ; bucket < counter.length ; bucket ++ ){
            if(total + counter[bucket] >= index){
                break;
            }
            total += counter[bucket];
        }
        if(bucket >= counter.length){
            return "";
        }
        QuickSelectUtil quickSelectUtil = this.selectUtilThreadLocal.get();
        if(quickSelectUtil == null){
            quickSelectUtil = new QuickSelectUtil(new File(workspaceDir),approximateMaxBucketCnt);
            this.selectUtilThreadLocal.set(quickSelectUtil);
        }
        long value = quickSelectUtil.select(table, column, bucket, index - total);
        return Long.toString(value);
    }

    public void removeTableData(String table){
        File folder = new File(workspaceDir, table);
        if(!folder.exists()){
            throw new RuntimeException("Cannot find data of table: " + table);
        }
        for(File f: folder.listFiles()){
            f.delete();
        }
    }

}