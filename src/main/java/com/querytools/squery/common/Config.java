package com.querytools.squery.common;

import org.apache.log4j.Logger;

import java.util.Properties;

public class Config {

    private static final Logger logger = Logger.getLogger(Config.class);

    private static final int K = 1024;
    private static final int M = 1024 * K;

    private static final int READ_BUFFER_SIZE_DEFAULT = 16 * M;
    private static final int BUFFER_SIZE_DEFAULT = 512 * K;
    private static final int PREPARE_BUF_SIZE_DEFAULT = 2 * K;
    private static final int BUCKET_MASK_BITS_DEFAULT = 6;

    private static final String CFG_PREFIX = "squery.config.";
    private static final String CFG_DEBUG = CFG_PREFIX + "debug";
    private static final String CFG_READ_BUFFER_SIZE = CFG_PREFIX + "readBufferSize";
    private static final String CFG_BUFFER_SIZE = CFG_PREFIX + "bufferSize";
    private static final String CFG_PREPARE_BUF_SIZE = CFG_PREFIX + "prepareBufferSize";
    private static final String CFG_BUCKET_MASK_BITS = CFG_PREFIX + "bucketMaskBits";

    public static int READ_BUFFER_SIZE = READ_BUFFER_SIZE_DEFAULT;
    public static int BUFFER_SIZE = BUFFER_SIZE_DEFAULT;
    public static int PREPARE_BUF_SIZE = PREPARE_BUF_SIZE_DEFAULT;
    public static int BUCKET_MASK_BITS = BUCKET_MASK_BITS_DEFAULT;
    public static int BUCKET_CNT = 1 << Config.BUCKET_MASK_BITS;

    public static void loadConfig(){
        boolean debug = Boolean.parseBoolean(System.getProperty(CFG_DEBUG, "false"));
        if(debug){
            logger.info("[Debug] Debug mode on.");
            BUFFER_SIZE = 128 * K;
        }
        Properties properties = System.getProperties();
        if(properties.containsKey(CFG_READ_BUFFER_SIZE)){
            READ_BUFFER_SIZE = Integer.parseInt(properties.getProperty(CFG_READ_BUFFER_SIZE));
        }
        if(properties.containsKey(CFG_BUFFER_SIZE)){
            BUFFER_SIZE = Integer.parseInt(properties.getProperty(CFG_BUFFER_SIZE));
        }
        if(properties.containsKey(CFG_PREPARE_BUF_SIZE)){
            PREPARE_BUF_SIZE = Integer.parseInt(properties.getProperty(CFG_PREPARE_BUF_SIZE));
        }
        if(properties.containsKey(CFG_BUCKET_MASK_BITS)){
            BUCKET_MASK_BITS = Integer.parseInt(properties.getProperty(CFG_BUCKET_MASK_BITS));
            BUCKET_CNT = 1 << Config.BUCKET_MASK_BITS;
        }
        logger.debug("Read Buffer Size: " + READ_BUFFER_SIZE);
        logger.debug("Normal Buffer Size: " + BUFFER_SIZE);
        logger.debug("Read Preparation Buffer Size: " + PREPARE_BUF_SIZE);
        logger.debug("Bucket mask bits: " + BUCKET_MASK_BITS);
    }

}
