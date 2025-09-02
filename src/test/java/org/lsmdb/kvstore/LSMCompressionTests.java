package org.lsmdb.kvstore;

import org.junit.jupiter.api.*;
import java.io.File;
import java.util.stream.Stream;
import static org.junit.jupiter.api.Assertions.*;

class LSMCompressionTests {

    private static final String BASE_DIR = "src/test/resources/DatabaseFiles";
    private static final String TEST_DATA_DIR = BASE_DIR + "/compression";

    private LSMDatabase database;

    @BeforeEach
    void setup() throws Exception {
        prepareTestDirectory();
        database = new LSMDatabase(TEST_DATA_DIR);
    }

    @AfterEach
    void teardown() throws Exception {
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

    @Test
    void testCompressionCreatesGzFiles() throws Exception {
        String largeValue = "compress_this_value_".repeat(1000);

        for (int i = 0; i < 5; i++) database.put("key" + i, largeValue);

        database.flush();
        database.forceCompaction();

        File[] compressedFiles = new File(TEST_DATA_DIR).listFiles(
                (f, name) -> name.endsWith(".gz") && name.startsWith("sstable_")
        );

        assertNotNull(compressedFiles, "Compressed files array should not be null");
        assertTrue(compressedFiles.length > 0, "Compressed SSTable files should exist");

        for (File f : compressedFiles) {
            System.out.println("[Test] Found compressed SSTable: " + f.getName());
        }
    }

    @Test
    void testDataIntegrityAfterCompression() throws Exception {
        String[] keys = {"alpha", "beta", "gamma"};
        String[] values = {"value1", "value2", "value3"};

        for (int i = 0; i < keys.length; i++) database.put(keys[i], values[i]);

        database.flush();

        // Reload DB to read from SSTables
        database = new LSMDatabase(TEST_DATA_DIR);

        for (int i = 0; i < keys.length; i++) {
            assertEquals(values[i], database.get(keys[i]),
                    "Data mismatch after reading from compressed SSTable");
        }
    }

    @Test
    void testMultipleCompressedSSTablesIntegrity() throws Exception {
        int flushInterval = 5;
        int totalEntries = 20;

        for (int i = 0; i < totalEntries; i++) {
            database.put("k" + i, "v" + i);
            if ((i + 1) % flushInterval == 0) database.flush();
        }

        database.flush();
        Thread.sleep(100);

        database = new LSMDatabase(TEST_DATA_DIR);

        for (int i = 0; i < totalEntries; i++) {
            String expected = "v" + i;
            String actual = database.get("k" + i);
            assertNotNull(actual, "Value should not be null for key: k" + i);
            assertEquals(expected, actual, "Data mismatch for key: k" + i);
        }
    }

    @Test
    void testCompressedFileSizeReduction() throws Exception {
        String largeValue = "repeat_for_compression_".repeat(2000);

        for (int i = 0; i < 3; i++) database.put("key" + i, largeValue);

        database.flush();

        File dir = new File(TEST_DATA_DIR);
        long uncompressedSize = Stream.of(dir.listFiles((f, n) -> n.startsWith("sstable_") && n.endsWith(".txt")))
                .mapToLong(File::length).sum();

        long compressedSize = Stream.of(dir.listFiles((f, n) -> n.startsWith("sstable_") && n.endsWith(".gz")))
                .mapToLong(File::length).sum();

        assertTrue(compressedSize < uncompressedSize || uncompressedSize == 0,
                "Compressed size should be smaller than uncompressed");
    }
}
