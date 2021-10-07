package com.querytools.squery.common;

import java.io.File;

public class FileUtil {

    public static boolean exists(String filePath){
        return (new File(filePath)).exists();
    }

    public static void createFolder(String folderPath){
        File folder = new File(folderPath);
        if(!folder.exists()){
            if(!folder.mkdirs()){
                throw new RuntimeException("Cannot create the folder: " + folderPath);
            }
        }
    }

}
