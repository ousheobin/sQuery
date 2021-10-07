package com.querytools.squery;

import com.querytools.squery.common.FileUtil;
import com.querytools.squery.common.TableRegistry;
import com.querytools.squery.io.IOManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;

public class SQueryToolImpl implements SQueryTool{

    private static final Logger logger = Logger.getLogger(SQueryToolImpl.class);
    private final TableRegistry tableRegistry = TableRegistry.getIntstance();

    private String workspaceDir;
    private IOManager ioManager;

    public SQueryToolImpl(String workspaceDir){
        this.workspaceDir = workspaceDir;
        if(!FileUtil.exists(workspaceDir)){
            FileUtil.createFolder(workspaceDir);
        }
        ioManager = new IOManager(workspaceDir);
        tableRegistry.setMetaDataFolder(workspaceDir + "/_tb_registry");
        tableRegistry.loadMetaData();
    }

    @Override
    public void loadData(String fileName, String tableName) throws Exception {
        if(!tableRegistry.containsTable(tableName)){
            File dataFile = new File(fileName);
            if(!dataFile.exists()){
                throw new RuntimeException("Data file: " + fileName + " not exists.");
            }
            String tbName = ioManager.load(dataFile, tableName);
            tableRegistry.dumpMetaData(tbName);
            ioManager.setApproximateMaxBucketCnt(tableRegistry.approximateMaxBucketCount());
        }else{
            logger.warn("Table '" + tableName + "' is exists. Will skip loading.");
        }
    }

    @Override
    public void loadDataSet(String dataSetFolder) throws Exception {
        File dir = new File(dataSetFolder);

        if(!dir.exists()){
            throw new IOException("Dataset not found: " + dataSetFolder);
        }

        File[] dataFiles = dir.listFiles(file -> {
            String fileName = file.getName();
            return !fileName.contains(".") || fileName.endsWith(".csv");
        });

        assert dataFiles != null;
        for (File dataFile : dataFiles) {
            String tableName = dataFile.getName();
            if(tableName.contains(".")){
                tableName = tableName.substring(0, tableName.lastIndexOf('.'));
            }
            if(tableRegistry.containsTable(tableName)){
                logger.warn("Table '" + tableName + "' is exists. Will skip loading.");
                continue;
            }
            loadData(dataFile.getAbsolutePath(), tableName);
        }
    }

    @Override
    public void removeTable(String tableName) throws Exception {
        if(!tableRegistry.containsTable(tableName)){
            throw new RuntimeException("Table '" + tableName + "' not exists");
        }
        tableRegistry.removeTable(tableName);
        ioManager.removeTableData(tableName);
        logger.info("Removed table: " + tableName);
    }

    @Override
    public String quantile(String table, String column, double percentile) throws Exception {
        if(ioManager == null){
            throw new RuntimeException("Not init");
        }
        int tableSize = tableRegistry.getTableLineCnt(table);
        int rank = (int) Math.round(tableSize * percentile);
        logger.debug("Read [" + table + "," + column + "," + percentile + "]" );
        return ioManager.readByIndex(table,column,rank);
    }

}
