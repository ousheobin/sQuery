package com.querytools.squery;

public interface SQueryTool {

    void loadData(String fileName, String tableName) throws Exception;

    void loadDataSet(String dataSetFolder) throws Exception;

    void removeTable(String tableName) throws Exception;

    String quantile(String table, String column, double percentile) throws Exception;

}
