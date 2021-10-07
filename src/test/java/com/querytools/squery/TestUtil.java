package com.querytools.squery;

import java.io.File;

public class TestUtil {

    protected static boolean deleteAllFile(File dir) {
        if (!dir.exists()) {
            return false;
        }
        if (dir.isFile()) {
            return dir.delete();
        } else {
            for (File file : dir.listFiles()) {
                deleteAllFile(file);
            }
        }
        return dir.delete();
    }

}
