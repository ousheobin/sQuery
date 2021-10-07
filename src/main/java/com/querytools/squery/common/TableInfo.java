package com.querytools.squery.common;

import java.io.Serializable;
import java.util.Map;

public class TableInfo implements Serializable {

    private String tableName;
    private int tableLineCnt;
    private Map<String, int[]> bucketCnt;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getTableLineCnt() {
        return tableLineCnt;
    }

    public void setTableLineCnt(int tableLineCnt) {
        this.tableLineCnt = tableLineCnt;
    }

    public void setBucketCnt(Map<String, int[]> bucketCnt) {
        this.bucketCnt = bucketCnt;
    }

    public Map<String, int[]> getBucketCnt() {
        return bucketCnt;
    }

    @Override
    public String toString() {
        return "TableInfo{" +
                "tableName='" + tableName + '\'' +
                ", tableLineCnt=" + tableLineCnt +
                '}';
    }
}