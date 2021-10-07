package com.querytools.squery.common;

import java.io.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TableRegistry {

    private static TableRegistry tableRegistry = null;

    private ConcurrentHashMap<String, TableInfo> tableInfoMap = new ConcurrentHashMap<String, TableInfo>();
    private String metaDataFolder;

    private TableRegistry(){

    }

    public static TableRegistry getIntstance(){
        if(tableRegistry == null){
            tableRegistry = new TableRegistry();
        }
        return tableRegistry;
    }

    public void setMetaDataFolder(String folder){
        this.metaDataFolder = folder;
    }

    public void addTable(TableInfo tableInfo){
        tableInfoMap.put(tableInfo.getTableName(),tableInfo);
    }

    public int getTableLineCnt(String name){
        if(tableInfoMap.containsKey(name)){
            return tableInfoMap.get(name).getTableLineCnt();
        }
        throw new IllegalArgumentException("unknow table name: " + name);
    }

    public int[] getColumnBucketCount(String table, String column){
        if(tableInfoMap.containsKey(table)){
            return tableInfoMap.get(table).getBucketCnt().get(column);
        }
        throw new IllegalArgumentException("unknow table name: " + table);
    }

    public void dumpMetaData(String tbName){
        File folder = new File(this.metaDataFolder);
        if(!folder.exists()) {
            folder.mkdirs();
        }
        if(!tableInfoMap.containsKey(tbName)){
            throw new RuntimeException("Cannot find the table: " + tbName);
        }
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(new File(folder,tbName));
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(tableInfoMap.get(tbName));
            oos.close();
            fos.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public boolean loadMetaData(){
        File folder = new File(this.metaDataFolder);
        if(!folder.exists()){
            return false;
        }
        File[] files = folder.listFiles();
        if(files == null || files.length == 0){
            return false;
        }
        for (File f: files){
            String name = f.getName();
            try{
                FileInputStream is = new FileInputStream(f);
                ObjectInputStream ois = new ObjectInputStream(is);
                TableInfo info = (TableInfo) ois.readObject();
                this.tableInfoMap.put(name,info);
                ois.close();
                is.close();
            }catch (Exception ex){
                ex.printStackTrace();
                this.tableInfoMap.clear();
                return false;
            }
        }
        return true;
    }

    public int approximateMaxBucketCount(){
        int max = Integer.MIN_VALUE;
        for(String tb: tableInfoMap.keySet()){
            Map<String, int[]> bucketCntMap = tableInfoMap.get(tb).getBucketCnt();
            for(String column: bucketCntMap.keySet()){
                int[] columnCnt = bucketCntMap.get(column);
                for (int i = 0; i < columnCnt.length; i ++ ){
                    max = Math.max(columnCnt[i], max);
                }
            }
        }
        return max;
    }

    public boolean containsTable(String table){
        return tableInfoMap.containsKey(table);
    }

    public void removeTable(String table){
        tableInfoMap.remove(table);
        File metaFile = new File(this.metaDataFolder, table);
        if(metaFile.exists()){
            metaFile.delete();
        }
    }

}
