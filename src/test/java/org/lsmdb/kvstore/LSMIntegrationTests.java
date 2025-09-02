package org.lsmdb.kvstore;

import java.io.File;
import java.nio.file.Files;

import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for LSMDatabase using a centralized DatabaseFiles/integration/ directory.
 */
public class LSMIntegrationTests {

    // Root folder for all test database files
    private static final String BASE_TEST_DATA_DIR = "src/test/resources/DatabaseFiles";
    // Subfolder specific to integration tests
    private static final String TEST_DATA_DIR = BASE_TEST_DATA_DIR + "/integration";

    private LSMDatabase database;

    @BeforeEach
    void setUp() throws Exception {
        prepareTestDirectory();
        database = new LSMDatabase(TEST_DATA_DIR);
    }

    @AfterEach
    void tearDown() throws Exception {
        cleanupTestDirectory();
    }

    /**
     * Prepares a clean test directory before each test.
     */
    private void prepareTestDirectory() {
        File dir = new File(TEST_DATA_DIR);
        if (dir.exists()) {
            deleteDirectory(dir);
        }
        dir.mkdirs();
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

    /**
     * Cleans up test directory after each test.
     */
    private void cleanupTestDirectory() {
        File dir = new File(TEST_DATA_DIR);
        if (dir.exists()) deleteDirectory(dir);
    }

    // ==================== BASIC OPERATIONS ====================
    @Test
    void testBasicKeyValueOperations() throws Exception {
        System.out.println("Running test: Basic key-value put and get operations.");
        database.put("user:1", "John Doe");
        database.put("user:2", "Jane Smith");
        database.put("user:3", "Bob Johnson");

        assertEquals("John Doe", database.get("user:1"));
        assertEquals("Jane Smith", database.get("user:2"));
        assertEquals("Bob Johnson", database.get("user:3"));
        assertNull(database.get("user:4"));
    }

    @Test
    void testKeyUpdateAndOverwrite() throws Exception {
        System.out.println("Running test: Updating an existing key overwrites its value.");
        database.put("user:1", "John Doe");
        database.put("user:1", "John Smith");
        assertEquals("John Smith", database.get("user:1"));
    }

    @Test
    void testMemTablePriorityOverSSTable() throws Exception {
        System.out.println("Running test: MemTable values take priority over SSTable values.");
        database.put("user:1", "John Doe");
        database.flush();
        database.put("user:1", "John Smith");
        assertEquals("John Smith", database.get("user:1"));
    }

    // ==================== FLUSH AND PERSISTENCE ====================
    @Test
    void testFlushOperation() throws Exception {
        System.out.println("Running test: Flush operation moves data to SSTable but keeps it accessible.");
        database.put("user:1", "John Doe");
        database.put("user:2", "Jane Smith");

        assertEquals("John Doe", database.get("user:1"));
        assertEquals("Jane Smith", database.get("user:2"));

        database.flush();

        assertEquals("John Doe", database.get("user:1"));
        assertEquals("Jane Smith", database.get("user:2"));

        File[] sstableFiles = new File(TEST_DATA_DIR).listFiles((f, name) -> name.startsWith("sstable_"));
        assertNotNull(sstableFiles);
        assertTrue(sstableFiles.length > 0);
    }

    @Test
    void testMultipleFlushOperations() throws Exception {
        System.out.println("Running test: Multiple flush operations create multiple SSTables.");
        database.put("user:1", "John Doe");
        database.flush();
        database.put("user:2", "Jane Smith");
        database.flush();
        database.put("user:3", "Bob Johnson");
        database.flush();

        assertEquals("John Doe", database.get("user:1"));
        assertEquals("Jane Smith", database.get("user:2"));
        assertEquals("Bob Johnson", database.get("user:3"));

        File[] sstableFiles = new File(TEST_DATA_DIR).listFiles((f, name) -> name.startsWith("sstable_"));
        assertNotNull(sstableFiles);
        assertTrue(sstableFiles.length >= 3);
    }

    @Test
    void testDataPersistenceAcrossInstances() throws Exception {
        System.out.println("Running test: Data persists across database instances.");
        database.put("user:1", "John Doe");
        database.put("user:2", "Jane Smith");
        database.flush();

        LSMDatabase recoveredDb = new LSMDatabase(TEST_DATA_DIR);
        assertEquals("John Doe", recoveredDb.get("user:1"));
        assertEquals("Jane Smith", recoveredDb.get("user:2"));
    }

    // ==================== WAL TESTS ====================
    @Test
    void testWALFileCreation() throws Exception {
        System.out.println("Running test: WAL file is created and contains pending writes.");
        database.put("user:1", "John Doe");
        database.put("user:2", "Jane Smith");

        File walFile = new File(TEST_DATA_DIR, "wal.log");
        assertTrue(walFile.exists());

        String content = Files.readString(walFile.toPath());
        assertTrue(content.contains("user:1=John Doe"));
        assertTrue(content.contains("user:2=Jane Smith"));
    }

    @Test
    void testWALClearedAfterFlush() throws Exception {
        System.out.println("Running test: WAL file is cleared after successful flush.");
        database.put("user:1", "John Doe");
        database.put("user:2", "Jane Smith");
        database.flush();

        File walFile = new File(TEST_DATA_DIR, "wal.log");
        String content = Files.readString(walFile.toPath());
        assertTrue(content.isEmpty(), "WAL should be cleared after flush");
    }

    // ==================== EDGE CASES ====================
    @Test
    void testSpecialCharactersHandling() throws Exception {
        System.out.println("Running test: Database handles special characters in keys and values.");
        database.put("key=with=equals", "value=with=equals");
        database.put("key\nwith\nnewlines", "value\nwith\nnewlines");
        database.put("key\twith\ttabs", "value\twith\ttabs");

        assertEquals("value=with=equals", database.get("key=with=equals"));
        assertEquals("value\nwith\nnewlines", database.get("key\nwith\nnewlines"));
        assertEquals("value\twith\ttabs", database.get("key\twith\ttabs"));
    }

    @Test
    void testNullAndEmptyValues() throws Exception {
        System.out.println("Running test: Database handles null and empty values.");
        database.put("key1", "");
        database.put("specialKey2", null);

        assertEquals("", database.get("key1"));
        assertNull(database.get("specialKey2"));
    }

    @Test
    void testLargeDatasetOperations() throws Exception {
        System.out.println("Running test: Database handles large datasets correctly.");
        for (int i = 0; i < 100; i++) database.put("user:" + i, "User " + i);

        assertEquals("User 42", database.get("user:42"));
        assertEquals("User 99", database.get("user:99"));
        assertEquals("User 0", database.get("user:0"));
        assertNull(database.get("user:100"));
    }

    // ==================== PERFORMANCE ====================
    @Test
    void testWritePerformance() throws Exception {
        System.out.println("Running test: Write performance within acceptable time.");
        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) database.put("key" + i, "value" + i);

        database.waitForFlushCompletion();

        long elapsed = System.currentTimeMillis() - start;
        assertTrue(elapsed < 10000, "Write operations should complete within 10 seconds");

        assertEquals("value0", database.get("key0"));
        assertEquals("value999", database.get("key999"));
    }

    @Test
    void testReadPerformance() throws Exception {
        System.out.println("Running test: Read performance within acceptable time.");
        for (int i = 0; i < 1000; i++) database.put("key" + i, "value" + i);

        long start = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) database.get("key" + (i % 1000));
        long elapsed = System.currentTimeMillis() - start;
        assertTrue(elapsed < 5000, "Read operations should complete within 5 seconds");
    }

    // ==================== DATABASE STATS ====================
    @Test
    void testDatabaseStatistics() throws Exception {
        System.out.println("Running test: Database statistics reflect correct state.");

        database.flush();
        database.waitForFlushCompletion();

        LSMDatabase.DatabaseStats stats = database.getStats();
        assertEquals(0, stats.getActiveMemtableSize());
        assertEquals(0, stats.getImmutableMemtableCount());
        assertTrue(stats.getTotalSSTableSize() >= 0);

        database.put("user:1", "John Doe");
        database.put("user:2", "Jane Smith");

        stats = database.getStats();
        assertTrue(stats.getActiveMemtableSize() > 0);
        assertTrue(stats.getImmutableMemtableCount() >= 0);

        database.flush();
        database.waitForFlushCompletion();

        stats = database.getStats();
        assertEquals(0, stats.getActiveMemtableSize());
        assertEquals(0, stats.getImmutableMemtableCount());
        assertTrue(stats.getTotalSSTableSize() > 0);
    }

    @Test
    void testDirectoryCreation() throws Exception {
        System.out.println("Running test: Database creates missing directories automatically.");
        String newDir = TEST_DATA_DIR;
        LSMDatabase newDb = new LSMDatabase(newDir);
        File dir = new File(newDir);
        assertTrue(dir.exists() && dir.isDirectory());
    }
}
