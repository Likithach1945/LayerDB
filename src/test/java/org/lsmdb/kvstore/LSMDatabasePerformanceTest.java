package org.lsmdb.kvstore;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lsmdb.kvstore.LSMDatabase;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

public class LSMDatabasePerformanceTest {

    private static LSMDatabase database;
    private static final String TEST_DIR = "./LSMPerformanceTest";

    @BeforeEach
    public void refreshDataStore() throws Exception {
        if (database != null) {
            database.waitForFlushCompletion();
            database.close();  // ✅ Close DB + WAL + SSTables
        }

        File dir = new File(TEST_DIR);
        if (!dir.exists()) dir.mkdirs();
        for (File file : dir.listFiles()) {
            file.delete();
        }

        database = new LSMDatabase(TEST_DIR);
    }


    @Test
    public void testPerformance() throws InvocationTargetException, IllegalAccessException, Exception {
        Class<?> clazz = this.getClass();
        Method[] methods = clazz.getDeclaredMethods();

        for (Method method : methods) {
            if (Modifier.isStatic(method.getModifiers())) {
                long startTime = System.nanoTime();
                method.invoke(this);
                long endTime = System.nanoTime();
                long duration = endTime - startTime;

                System.out.println("\n" + method.getName() + " took:");
                System.out.println(duration + " nanoseconds");
                System.out.println(duration / 1e6 + " milliseconds");
                System.out.println(duration / 1e9 + " seconds");

                // Reset database after each test
                refreshDataStore();

            }
        }
    }

    static void insertHundredKeys() throws Exception {
        for (int i = 0; i < 100; i++) {
            database.put("key-" + i, "value-" + i);
        }
    }

    static void insertFiveHundredKeys() throws Exception {
        for (int i = 0; i < 500; i++) {
            database.put("key-" + i, "value-" + i);
        }
    }

    static void insertThousandKeys() throws Exception {
        for (int i = 0; i < 1000; i++) {
            database.put("key-" + i, "value-" + i);
        }
    }

    static void insertFiveThousandKeys() throws Exception {
        for (int i = 0; i < 5000; i++) {
            database.put("key-" + i, "value-" + i);
        }
    }

    static void insertTenThousandKeys() throws Exception {
        for (int i = 0; i < 10_000; i++) {
            database.put("key-" + i, "value-" + i);
        }
    }

    static void insertTwentyThousandKeys() throws Exception {
        for (int i = 0; i < 20_000; i++) {
            database.put("key-" + i, "value-" + i);
        }
    }
}
