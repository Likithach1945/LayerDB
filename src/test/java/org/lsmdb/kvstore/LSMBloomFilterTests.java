package org.lsmdb.kvstore;

import org.junit.jupiter.api.*;
import java.io.File;
import static org.junit.jupiter.api.Assertions.*;

class LSMBloomFilterTests {

    private static final String TEST_DATA_DIR = "src/test/resources/DatabaseFiles/bloomFilter";
    private LSMDatabase database;

    @BeforeEach
    void setUp() throws Exception {
        cleanupTestData();
        new File(TEST_DATA_DIR).mkdirs();
        database = new LSMDatabase(TEST_DATA_DIR);
    }

    @AfterEach
    void tearDown() {
        cleanupTestData();
    }

    private void cleanupTestData() {
        File dir = new File(TEST_DATA_DIR);
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File f : files) f.delete();
            }
            dir.delete();
        }
    }

    @Test
    void testBloomFilterContainsInsertedKeys() throws Exception {
        System.out.println("Testing that Bloom filter contains inserted keys...");

        database.put("key1", "value1");
        database.put("key2", "value2");
        database.put("key3", "value3");

        database.flush(); // force SSTable creation

        // Check Bloom filter positive hits
        assertTrue(database.mightContainInSSTables("key1"));
        assertTrue(database.mightContainInSSTables("key2"));
        assertTrue(database.mightContainInSSTables("key3"));
    }

    @Test
    void testBloomFilterRejectsNonExistentKeys() throws Exception {
        System.out.println("Testing that Bloom filter rejects non-existent keys...");

        database.put("keyA", "valueA");
        database.put("keyB", "valueB");

        database.flush();

        // Check Bloom filter negative hits (may rarely be false positives)
        assertFalse(database.mightContainInSSTables("keyX"));
        assertFalse(database.mightContainInSSTables("keyY"));
    }

    @Test
    void testBloomFilterAfterMultipleFlushes() throws Exception {
        System.out.println("Testing Bloom filter correctness after multiple flushes...");

        for (int i = 0; i < 50; i++) {
            database.put("key" + i, "value" + i);
            if (i % 10 == 0) database.flush(); // flush periodically
        }

        // Flush remaining keys in the active MemTable
        database.flush();

        // All inserted keys should be detected by Bloom filter
        for (int i = 0; i < 50; i++) {
            assertTrue(database.mightContainInSSTables("key" + i), "Bloom filter should contain key" + i);
        }

        // Some non-existent keys
        assertFalse(database.mightContainInSSTables("not_in_db_1"));
        assertFalse(database.mightContainInSSTables("not_in_db_2"));
    }

    @Test
    void testBloomFilterOnEmptyDatabase() throws Exception {
        assertFalse(database.mightContainInSSTables("randomKey"),
                "Empty DB should not report any key present");
    }

    @Test
    void testBloomFilterPersistenceAcrossRestart() throws Exception {
        database.put("hello", "world");
        database.flush();

        database = new LSMDatabase(TEST_DATA_DIR);
        assertTrue(database.mightContainInSSTables("hello"),
                "Bloom filter should persist across restart");
    }

    @Test
    void testBloomFilterFalsePositiveRate() throws Exception {
        for (int i = 0; i < 1000; i++) {
            database.put("key" + i, "val" + i);
        }
        database.flush();

        int falsePositives = 0;
        for (int i = 0; i < 1000; i++) {
            if (database.mightContainInSSTables("absent" + i)) {
                falsePositives++;
            }
        }
        double rate = falsePositives / 1000.0;
        System.out.println("False positive rate: " + rate);
        assertTrue(rate < 0.1, "False positive rate should be reasonably low");
    }


}
