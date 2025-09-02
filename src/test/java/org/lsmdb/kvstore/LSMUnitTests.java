package org.lsmdb.kvstore;

import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.nio.file.Files;

class LSMUnitTests {

    private static final String BASE_DIR = "src/test/resources/DatabaseFiles";
    private static final String TEST_DIR = BASE_DIR + "/unit";

    private LSMDatabase db;

    @BeforeEach
    void setUp() throws Exception {
        prepareTestDirectory();
        db = new LSMDatabase(TEST_DIR);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (db != null) {
            db.waitForFlushCompletion();
            db.close();
            db = null;
        }
        cleanupTestDirectory();
    }

    private void prepareTestDirectory() {
        File dir = new File(TEST_DIR);
        if (dir.exists()) deleteDirectory(dir);
        dir.mkdirs();
    }

    private void cleanupTestDirectory() {
        File dir = new File(TEST_DIR);
        if (dir.exists()) deleteDirectory(dir);
    }

    private void deleteDirectory(File dir) {
        for (File f : dir.listFiles()) {
            if (f.isDirectory()) deleteDirectory(f);
            else f.delete();
        }
        dir.delete();
    }

    // ================= MEMTABLE & WAL Tests =================
    @Test
    void testPutAndGet() throws Exception {
        db.put("key1", "value1");
        db.put("key2", "value2");
        assertEquals("value1", db.get("key1"));
        assertEquals("value2", db.get("key2"));
        assertNull(db.get("key3"));
    }

    @Test
    void testOverwriteValue() throws Exception {
        db.put("key1", "v1");
        db.put("key1", "v2");
        assertEquals("v2", db.get("key1"));
    }

    @Test
    void testFlushWritesSSTable() throws Exception {
        db.put("keyA", "A");
        db.put("keyB", "B");
        db.flush();

        File sstableFile = new File(TEST_DIR, "sstable_0.txt");
        assertTrue(sstableFile.exists());

        String content = Files.readString(sstableFile.toPath());
        assertTrue(content.contains("keyA=A"));
        assertTrue(content.contains("keyB=B"));
    }

    // ================= SSTable Lookup =================
    @Test
    void testLookupPlainSSTable() throws Exception {
        db.put("k1", "v1");
        db.flush();
        assertEquals("v1", db.get("k1"));
    }

    @Test
    void testLookupCompressedSSTable() throws Exception {
        db.put("x1", "v1");
        db.flush();
        db.forceCompaction(); // forces creation of compressed .gz

        String val = SSTable.lookupCompressed(TEST_DIR, "x1");
        assertEquals("v1", val);
    }

    @Test
    void testGetChecksMemTableAndSSTables() throws Exception {
        db.put("a", "1");  // active memtable
        db.flush();        // moves to SSTable
        db.put("b", "2");  // active memtable

        assertEquals("1", db.get("a")); // from SSTable
        assertEquals("2", db.get("b")); // from memtable
    }

    // ================= Compaction =================
    @Test
    void testCompactBySizeAndFileCount() throws Exception {
        for (int i = 0; i < 5; i++) {
            db.put("k" + i, "v" + i);
            db.flush();
        }

        File[] txtFiles = new File(TEST_DIR).listFiles(f -> f.getName().endsWith(".txt") && !f.getName().contains("compacted"));
        assertTrue(txtFiles.length >= 1);

        db.forceCompaction();

        File compacted = new File(TEST_DIR, "sstable_compacted.txt.gz");
        assertTrue(compacted.exists());

        for (int i = 0; i < 5; i++) {
            assertEquals("v" + i, SSTable.lookupCompressed(TEST_DIR, "k" + i));
        }
    }

    @Test
    void testGetCompressionStats() throws Exception {
        db.put("a", "1");
        db.put("b", "2");
        db.flush();

        SSTable.CompressionStats stats = SSTable.getCompressionStats(TEST_DIR);
        assertTrue(stats.getTotalFiles() > 0);
        assertTrue(stats.getCompressedFiles() >= 0);
        assertTrue(stats.getTotalSize() > 0);
    }

    // ================= Database Stats =================
    @Test
    void testDatabaseStats() throws Exception {
        db.put("k", "v");
        db.flush();

        LSMDatabase.DatabaseStats stats = db.getStats();
        assertTrue(stats.getActiveMemtableSize() >= 0);
        assertTrue(stats.getImmutableMemtableCount() >= 0);
        assertTrue(stats.getTotalSSTableSize() > 0);
        assertTrue(stats.getSstableFileCount() > 0);
    }

    // ================= Edge Cases =================
    @Test
    void testEmptyGet() throws Exception {
        assertNull(db.get("nonexistent"));
    }

    @Test
    void testNullAndEmptyValues() throws Exception {
        db.put("empty", "");
        db.put("nullval", null);

        assertEquals("", db.get("empty"));
        assertNull(db.get("nullval"));
    }
}
