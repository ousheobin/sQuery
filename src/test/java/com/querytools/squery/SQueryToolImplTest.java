package com.querytools.squery;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

public class SQueryToolImplTest {

    File testDataDir = new File("./test_data");
    File testWorkspaceDir = new File("./tmp_workspace");
    File testResultsFile = new File("./test_result/results");

    @Before
    public void setup(){
        if (testWorkspaceDir.exists()) {
            TestUtil.deleteAllFile(testWorkspaceDir);
            testWorkspaceDir.delete();
        }
        testWorkspaceDir.mkdir();
    }

    @After
    public void tearDown(){
        TestUtil.deleteAllFile(testWorkspaceDir);
        testWorkspaceDir.delete();
    }

    @Test
    public void doTest() throws Exception {

        List<String> results = new ArrayList<>();

        try (BufferedReader resReader = new BufferedReader(new FileReader(testResultsFile))) {
            String line;
            while ((line = resReader.readLine()) != null) {
                results.add(line);
            }
        }

        testFirstLoad(results);
        testRecoveryFromMetadata(results);

    }


    private void testFirstLoad(List<String> results) throws Exception {
        SQueryTool queryTool = new SQueryToolImpl(testWorkspaceDir.getAbsolutePath());
        queryTool.loadDataSet(testDataDir.getAbsolutePath());
        testQuery(queryTool, results, 10);
    }

    private void testRecoveryFromMetadata(List<String> results) throws Exception{
        SQueryTool sQueryTool = new SQueryToolImpl(testWorkspaceDir.getAbsolutePath());
        sQueryTool.loadDataSet(testDataDir.getAbsolutePath());

        int taskCnt = Runtime.getRuntime().availableProcessors() * 2;
        Executor threadPool = Executors.newFixedThreadPool(taskCnt);
        CompletableFuture[] futures = new CompletableFuture[taskCnt];


        for (int i = 0; i < taskCnt; i++) {
            futures[i] = CompletableFuture.runAsync(
                    () -> testQuery(sQueryTool, results, 500),
                    threadPool
            );
        }

        CompletableFuture.allOf(futures).get();
    }

    private void testQuery(SQueryTool sQueryTool, List<String> results, int testCount) {
        try {
            for (int i = 0; i < testCount; i++) {
                int p = ThreadLocalRandom.current().nextInt(results.size());
                String[] resultStr = results.get(p).split(" ");
                String table = resultStr[0];
                String column = resultStr[1];
                double percentile = Double.parseDouble(resultStr[2]);
                String answer = resultStr[3];

                Assert.assertEquals(answer, sQueryTool.quantile(table, column, percentile));
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

}