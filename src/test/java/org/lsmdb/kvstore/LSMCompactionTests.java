package org.lsmdb.kvstore;

import java.io.File;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

/**
 * Updated tests for LSMDatabase with separate size-based and file-count-based compaction.
 * Uses a centralized DatabaseFiles/compaction/ directory for storing SSTables, WALs, and indexes.
 */
public class LSMCompactionTests {

    // Root folder for all test-related database files
    private static final String BASE_TEST_DATA_DIR = "src/test/resources/DatabaseFiles";
    // Subfolder specific to this test class
    private static final String TEST_DATA_DIR = BASE_TEST_DATA_DIR + "/compaction";

    private LSMDatabase database;

    @BeforeEach
    void setUp() throws Exception {
        prepareTestDirectory();
        database = new LSMDatabase(TEST_DATA_DIR);
    }

    /**
     * Ensures a clean test directory for compaction tests.
     */
    private void prepareTestDirectory() {
        File dir = new File(TEST_DATA_DIR);
        if (dir.exists()) {
            for (File file : dir.listFiles()) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
        } else {
            dir.mkdirs();
        }
    }

    /**
     * Recursively deletes a directory.
     */
    private void deleteDirectory(File dir) {
        for (File file : dir.listFiles()) {
            if (file.isDirectory()) {
                deleteDirectory(file);
            } else {
                file.delete();
            }
        }
        dir.delete();
    }

    // ==================== FILE COUNT TRIGGER ====================

    @Test
    void testCompactionTriggeredByFileCount() throws Exception {
        System.out.println("Testing compaction triggered by file count...");

        // Insert 10 keys and flush each to create separate SSTables
        for (int i = 0; i < 10; i++) {
            database.put("key" + i, "value" + i);
            database.flush();
        }

        // Wait until immutable memtables are flushed
        while (database.getStats().getImmutableMemtableCount() > 0) {
            Thread.sleep(50);
        }

        // Count SSTables before compaction
        File dir = new File(TEST_DATA_DIR);
        int beforeCount = Arrays.stream(dir.listFiles((f, name) -> name.startsWith("sstable_"))).toArray().length;
        System.out.println("Before compaction: " + beforeCount + " SSTable files");

        // Force compaction
        database.forceCompaction();
        Thread.sleep(200);

        // Count SSTables after compaction
        int afterCount = Arrays.stream(dir.listFiles((f, name) -> name.startsWith("sstable_"))).toArray().length;
        System.out.println("After compaction: " + afterCount + " SSTable files");

        // Verify data integrity
        for (int i = 0; i < 10; i++) {
            assertEquals("value" + i, database.get("key" + i), "Key mismatch for key" + i);
        }

        assertTrue(afterCount <= beforeCount, "File count did not reduce after compaction");
    }

    // ==================== SIZE TRIGGER ====================

    @Test
    void testAutomaticCompactionWhenSizeLimitExceeded() throws Exception {
        System.out.println("Testing automatic compaction triggered by size limit...");

        String largeValue = "large_value".repeat(100_000); // ~100KB per value
        int recordCount = 0;

        // Insert until SSTable size exceeds ~10 MB
        while (true) {
            database.put("large_key_" + recordCount, largeValue);
            recordCount++;
            if (recordCount % 10 == 0) database.flush();

            LSMDatabase.DatabaseStats stats = database.getStats();
            if (stats.getTotalSSTableSize() > 10 * 1024 * 1024) break;
        }

        LSMDatabase.DatabaseStats beforeStats = database.getStats();
        System.out.println("[Before Compaction] " + beforeStats);

        database.waitForFlushCompletion();
        database.forceCompaction();

        LSMDatabase.DatabaseStats afterStats = database.getStats();
        System.out.println("[After Compaction]  " + afterStats);

        assertTrue(afterStats.getSstableFileCount() <= beforeStats.getSstableFileCount(),
                "SSTable file count should decrease or stay the same after compaction");

        for (int i = 0; i < recordCount; i++) {
            assertEquals(largeValue, database.get("large_key_" + i),
                    "Data mismatch for key: large_key_" + i);
        }
    }

    // ==================== COMPRESSION TESTS ====================

    @Test
    void testCompressionEffectiveness() throws Exception {
        String repetitiveValue = "repeat_".repeat(100);
        for (int i = 0; i < 50; i++) database.put("key" + i, repetitiveValue);

        database.forceCompaction();

        SSTable.CompressionStats stats = SSTable.getCompressionStats(TEST_DATA_DIR);
        assertTrue(stats.getCompressedFiles() > 0);
        assertTrue(stats.getCompressionRatio() < 100);

        assertEquals(repetitiveValue, database.get("key0"));
        assertEquals(repetitiveValue, database.get("key49"));
    }

    @Test
    void testCompressionWithMixedData() throws Exception {
        for (int i = 0; i < 20; i++)
            database.put("rep_key_" + i, "repeat_".repeat(50));
        for (int i = 0; i < 20; i++)
            database.put("uniq_key_" + i, "uniq_" + System.nanoTime());

        database.waitForFlushCompletion();
        database.forceCompaction();

        for (int i = 0; i < 20; i++)
            assertEquals("repeat_".repeat(50), database.get("rep_key_" + i));
    }

    // ==================== LARGE DATASET ====================

    @Test
    @Order(0)
    public void testLargeDatasetCompaction() throws Exception {
        for (int i = 0; i < 500; i++)
            database.put("key_" + i, "value_" + i + "_".repeat(10));

        database.forceCompaction();
        database.waitForFlushCompletion();

        database.rebuildBloomFilterFromDisk();

        for (int i = 0; i < 500; i += 50)
            assertEquals("value_" + i + "_".repeat(10), database.get("key_" + i));
    }

    // ==================== OVERLAPPING KEYS ====================

    @Test
    void testCompactionWithOverlappingKeys() throws Exception {
        for (int batch = 0; batch < 5; batch++) {
            for (int i = 0; i < 10; i++)
                database.put("key_" + i, "value_batch_" + batch);
            database.flush();
        }
        database.forceCompaction();

        for (int i = 0; i < 10; i++)
            assertEquals("value_batch_4", database.get("key_" + i));
    }

    // ==================== PERFORMANCE ====================

    @Test
    void testCompactionPerformance() throws Exception {
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 300; i++)
            database.put("perf_key_" + i, "perf_value_" + i + "_".repeat(20));
        long writeTime = System.currentTimeMillis() - startTime;

        startTime = System.currentTimeMillis();
        database.forceCompaction();
        long compactionTime = System.currentTimeMillis() - startTime;

        assertTrue(writeTime < 10_000);
        assertTrue(compactionTime < 5_000);

        assertEquals("perf_value_0" + "_".repeat(20), database.get("perf_key_0"));
        assertEquals("perf_value_299" + "_".repeat(20), database.get("perf_key_299"));
    }

    // ==================== EDGE CASES ====================

    @Test
    void testCompactionWithEmptyDatabase() throws Exception {
        File[] filesBefore = new File(TEST_DATA_DIR).listFiles((f, name) -> !name.endsWith("wal.log"));
        int countBefore = filesBefore != null ? filesBefore.length : 0;
        assertEquals(0, countBefore);

        database.forceCompaction();

        File[] filesAfter = new File(TEST_DATA_DIR).listFiles((f, name) -> !name.endsWith("wal.log"));
        int countAfter = filesAfter != null ? filesAfter.length : 0;
        assertEquals(0, countAfter);
    }

    @Test
    void testCompactionWithSingleFile() throws Exception {
        database.put("single_key", "single_value");
        database.flush();

        database.forceCompaction();

        assertEquals("single_value", database.get("single_key"));
    }
}
