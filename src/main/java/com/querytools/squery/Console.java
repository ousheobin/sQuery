package com.querytools.squery;

import com.querytools.squery.common.Config;
import org.apache.log4j.Logger;

import java.io.File;

public class Console {

    private static final Logger logger = Logger.getLogger(Console.class);

    public static void main(String[] args) {
        Config.loadConfig();
        String workspace = windowsOS()?"C://squery/workspace":"/tmp/squery/workspace";
        if(System.getProperties().containsKey(Config.CFG_WORKSPACE)){
            workspace = System.getProperty(Config.CFG_WORKSPACE);
        }
        File workspaceFile = new File(workspace);
        if(!workspaceFile.exists()){
            if(!workspaceFile.mkdirs()){
                System.out.println("Cannot use " + workspaceFile +" as work space.");
                System.exit(1);
            }
        }

        logger.info("Now is using " + workspace + " as workspace");
        if(args.length == 0){
            System.out.println("Arguments is illegal");
            printHelp();
            System.exit(1);
        }

        SQueryTool sQueryTool = new SQueryToolImpl(workspace);

        switch (args[0]){
            case "load":
                if(args.length < 2){
                    System.out.println("You need to specify the data file or data set folder");
                    printHelp();
                    System.exit(1);
                }
                File target = new File(args[1]);
                if(!target.exists()){
                    System.out.println("Dataset/Data File: "+target+" not exists. Please check the path.");
                    System.exit(1);
                }
                if(target.isFile()){
                    try {
                        String tableName = "";
                        if(args.length > 2){
                            for (int i = 2 ; i < args.length ; i ++ ){
                                if("-name".equals(args[i])){
                                    if(i + 1 >= args.length){
                                        System.out.println("You need to specify the name of table");
                                        System.exit(1);
                                    }
                                    tableName = args[++i];
                                }
                            }
                        }

                        if(tableName == null || tableName.isEmpty()){
                            String fileName = target.getName();
                            if(fileName.endsWith(".csv")){
                                tableName = fileName.substring(0, fileName.lastIndexOf(".csv"));
                            }else if(fileName.contains(".")){
                                System.out.println("Your may need to use -name to specify the table name for " + target);
                                System.exit(0);
                            }
                        }

                        sQueryTool.loadData(target.getAbsolutePath(), tableName);

                    } catch (Exception ex) {
                        logger.error("Unable to load file. Reasons:", ex);
                    }
                }else{
                    try {
                        sQueryTool.loadDataSet(args[1]);
                    } catch (Exception ex) {
                        logger.error("Unable to load dataset. Reasons:", ex);
                    }
                }
                break;
            case "delete":
                if(args.length < 2){
                    System.out.println("You need to specify the table you are going to remove");
                    printHelp();
                    System.exit(1);
                }
                try {
                    sQueryTool.removeTable(args[1]);
                } catch (Exception ex) {
                    logger.error("Unable to remove table: '"+args[1]+"'. Reasons:", ex);
                }
                break;
            case "median":
                if(args.length < 3){
                    System.out.println("You need to specify the table and field you need to query");
                    printHelp();
                    System.exit(1);
                }
                try {
                    String res = sQueryTool.quantile(args[1],args[2],0.5);
                    System.out.print("Query Result: \n");
                    System.out.println(res);
                    System.out.print("\n");
                } catch (Exception ex) {
                    logger.error("Unable to perform the query. Reasons:", ex);
                    System.exit(1);
                }

                break;
            case "quantile":
                if(args.length < 4){
                    System.out.println("You need to specify the table, field and percentage you need to query");
                    printHelp();
                    System.exit(1);
                }

                try {
                    String percentage = args[3].trim();
                    double percentageVal = 0.0d;
                    if(!percentage.endsWith("%")){
                        percentageVal = Double.parseDouble(percentage);
                    }else{
                        percentageVal = Double.parseDouble(percentage.substring(0, percentage.length() - 1)) / 100;
                    }
                    String res = sQueryTool.quantile(args[1],args[2],percentageVal);
                    System.out.print("Query Result: \n\n");
                    System.out.println(res);
                    System.out.print("\n");
                } catch (Exception ex) {
                    logger.error("Unable to perform the query. Reasons:", ex);
                    System.exit(1);
                }
                break;
            default:
                System.out.println("Unknown operation: '"+args[0]+"'");
                printHelp();
                System.exit(1);
        }

    }

    private static void printHelp(){
        System.out.println("\nSQuery arguments:");
        System.out.println("load [dataFile | datasetFolder] (args...)");
        System.out.println("delete [tableName]");
        System.out.println("median [tableName] [filed]");
        System.out.println("quantile [tableName] [filed] xx.x%");
    }

    private static boolean windowsOS() {
        return System.getProperty("os.name").toLowerCase().contains("windows");
    }

}
