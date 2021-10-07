package com.querytools.squery.common;

public class Config {

    private static final boolean DEBUG = false;

    private static final int K = 1024;
    private static final int M = 1024 * K;

    public static final int READ_BUFFER_SIZE = 16 * M;
    public static final int BUFFER_SIZE = (DEBUG)?128 * K: 512 * K;
    public static final int PREPARE_BUF_SIZE = 2 * K;

    public static final int BUCKET_MASK_BITS = 6;
    public static final int BUCKET_CNT = 1 << Config.BUCKET_MASK_BITS;

}
