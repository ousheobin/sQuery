package com.querytools.squery.common;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class ThreadPool {

    private static ThreadPool instance;
    private static final Object LOCK = new Object();

    private ExecutorService threadPool;

    private ThreadPool(){
        threadPool = Executors.newCachedThreadPool(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setDaemon(true);
                return t;
            }
        });
    }

    public static ThreadPool getInstance(){
        if(instance == null){
            synchronized (LOCK){
                if(instance == null){
                    instance = new ThreadPool();
                }
            }
        }
        return instance;
    }

    public void submit(Runnable r){
        threadPool.submit(r);
    }

}
