package org.lsmdb.kvstore;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Stress tests for the LSM database under heavy load and edge conditions.
 * Uses centralized DatabaseFiles/stress/ folder.
 */
public class LSMStressTests {

    private static final String BASE_TEST_DATA_DIR = "src/test/resources/DatabaseFiles";
    private static final String TEST_DATA_DIR = BASE_TEST_DATA_DIR + "/stress";

    private LSMDatabase database;

    @BeforeEach
    void setUp() throws Exception {
        prepareTestDirectory();
        database = new LSMDatabase(TEST_DATA_DIR);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (database != null) {
            database.waitForFlushCompletion();
            database.close();
            database = null;
        }
        cleanupTestDirectory();
    }

    private void prepareTestDirectory() {
        File dir = new File(TEST_DATA_DIR);
        if (dir.exists()) deleteDirectory(dir);
        dir.mkdirs();
    }

    private void cleanupTestDirectory() {
        File dir = new File(TEST_DATA_DIR);
        if (dir.exists()) deleteDirectory(dir);
    }

    private void deleteDirectory(File dir) {
        for (File file : dir.listFiles()) {
            if (file.isDirectory()) deleteDirectory(file);
            else file.delete();
        }
        dir.delete();
    }

    // ==================== HEAVY WRITE LOAD TEST ====================
    @Test
    void testHeavyWriteLoad() throws Exception {
        System.out.println("Testing heavy write load...");
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < 5000; i++) {
            String key = "stress_key_" + i;
            String value = "stress_value_" + i + "_".repeat(10000);
            database.put(key, value);
        }

        long writeTime = System.currentTimeMillis() - startTime;
        System.out.println("Heavy write load completed in: " + writeTime + "ms");

        // Spot check data
        for (int i = 0; i < 5000; i += 500) {
            String expected = "stress_value_" + i + "_".repeat(10000);
            assertEquals(expected, database.get("stress_key_" + i));
        }

        assertTrue(writeTime < 15000, "Heavy write load should complete within 15 seconds");
    }

    // ==================== RAPID KEY UPDATE TEST ====================
    @Test
    void testRapidKeyUpdates() throws Exception {
        System.out.println("Testing rapid key updates...");
        String[] keys = {"update_key_1", "update_key_2", "update_key_3", "update_key_4", "update_key_5"};

        for (int iteration = 0; iteration < 100; iteration++) {
            for (String key : keys) {
                database.put(key, "updated_value_" + iteration);
            }
        }

        for (String key : keys) {
            assertEquals("updated_value_99", database.get(key));
        }
    }

    // ==================== MEMTABLE SIZE LIMIT TEST ====================
    @Test
    void testMemTableSizeLimitEnforcement() throws Exception {
        System.out.println("Testing MemTable size limit enforcement...");

        LSMDatabase.DatabaseStats stats = database.getStats();
        assertEquals(0, stats.getActiveMemtableSize(), "Active memtable should start at 0");

        int entryCount = 0;
        String largeValue = "large_value_".repeat(50);

        while(stats.getSstableFileCount() == 0) {
            database.put("size_test_key_" + entryCount, largeValue);
            entryCount++;
            stats = database.getStats();
            if (entryCount > 200) break;
        }

        database.waitForFlushCompletion();
        database.flush();

        stats = database.getStats();

        assertEquals(0, stats.getActiveMemtableSize(), "Active memtable should be empty after flush");
        assertTrue(stats.getSstableFileCount() > 0, "SSTables should have been created");

        for (int i = 0; i < entryCount; i++) {
            assertEquals(largeValue, database.get("size_test_key_" + i),
                    "Data mismatch for key size_test_key_" + i);
        }
    }

    // ==================== CONCURRENT ACCESS SIMULATION TEST ====================
    @Test
    void testConcurrentAccessPatterns() throws Exception {
        System.out.println("Testing concurrent access patterns...");

        int threadCount = 5;
        int keysPerThread = 100;

        for (int threadId = 0; threadId < threadCount; threadId++) {
            for (int i = 0; i < keysPerThread; i++) {
                String key = "thread_" + threadId + "_key_" + i;
                String value = "thread_" + threadId + "_value_" + i;
                database.put(key, value);
            }
        }

        database.waitForFlushCompletion();
        database.flush();
        Thread.sleep(100);

        for (int threadId = 0; threadId < threadCount; threadId++) {
            for (int i = 0; i < keysPerThread; i++) {
                String key = "thread_" + threadId + "_key_" + i;
                String expectedValue = "thread_" + threadId + "_value_" + i;
                assertEquals(expectedValue, database.get(key), "Data integrity check failed for key: " + key);
            }
        }
    }

    // ==================== DATABASE RECOVERY TEST ====================
    @Test
    void testDatabaseRecoveryScenario() throws Exception {
        System.out.println("Testing database recovery scenario...");

        database.put("recovery_key_1", "recovery_value_1");
        database.put("recovery_key_2", "recovery_value_2");
        database.flush();
        database.put("recovery_key_3", "recovery_value_3");
        database.put("recovery_key_1", "recovery_value_1_updated");

        assertEquals("recovery_value_1_updated", database.get("recovery_key_1"));
        assertEquals("recovery_value_2", database.get("recovery_key_2"));
        assertEquals("recovery_value_3", database.get("recovery_key_3"));

        database.put("recovery_key_4", "recovery_value_4");
        assertEquals("recovery_value_4", database.get("recovery_key_4"));
    }

    // ==================== LARGE VALUE TEST ====================
    @Test
    void testLargeValueHandling() throws Exception {
        System.out.println("Testing large value handling...");
        String large1 = "large_value_".repeat(1000);
        String large2 = "another_large_value_".repeat(100);

        database.put("large_key_1", large1);
        database.put("large_key_2", large2);

        assertEquals(large1, database.get("large_key_1"));
        assertEquals(large2, database.get("large_key_2"));

        database.waitForFlushCompletion();
        database.flush();

        assertEquals(large1, database.get("large_key_1"));
        assertEquals(large2, database.get("large_key_2"));
    }

    // ==================== PERFORMANCE BENCHMARK ====================
    @Test
    void testReadWritePerformanceBenchmark() throws Exception {
        System.out.println("Running performance benchmark...");

        long startWrite = System.currentTimeMillis();
        for (int i = 0; i < 2000; i++) database.put("benchmark_key_" + i, "benchmark_value_" + i);
        long writeTime = System.currentTimeMillis() - startWrite;

        database.waitForFlushCompletion();

        long startRead = System.currentTimeMillis();
        for (int i = 0; i < 2000; i++) database.get("benchmark_key_" + i);
        long readTime = System.currentTimeMillis() - startRead;

        database.waitForFlushCompletion();

        long startMixed = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            database.put("mixed_key_" + i, "mixed_value_" + i);
            database.get("benchmark_key_" + (i % 2000));
        }
        long mixedTime = System.currentTimeMillis() - startMixed;

        System.out.println("Performance Results:");
        System.out.println("  Write: " + writeTime + "ms");
        System.out.println("  Read: " + readTime + "ms");
        System.out.println("  Mixed: " + mixedTime + "ms");

        assertTrue(writeTime < 10000);
        assertTrue(readTime < 5000);
        assertTrue(mixedTime < 8000);

        assertEquals("benchmark_value_0", database.get("benchmark_key_0"));
        assertEquals("benchmark_value_1999", database.get("benchmark_key_1999"));
        assertEquals("mixed_value_999", database.get("mixed_key_999"));
    }
}
