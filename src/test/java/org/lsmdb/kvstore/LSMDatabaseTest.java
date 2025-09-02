package org.lsmdb.kvstore;

import org.junit.jupiter.api.*;
import java.io.File;
import java.io.IOException;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class LSMDatabaseTest {

    private static final String TEST_DIR = "src/test/resources/DatabaseFiles/LSMTestData";
    private LSMDatabase database;

    @BeforeEach
    public void setUp() throws Exception {
        File dir = new File(TEST_DIR);
        if (!dir.exists()) dir.mkdirs();
        for (File file : dir.listFiles()) {
            file.delete();
        }
        database = new LSMDatabase(TEST_DIR);
    }

    @AfterEach
    public void tearDown() throws Exception {
        database.close(); // We'll add this method if missing
    }

    // ---------- BASIC TESTS ---------- //

    @Test
    @Order(1)
    public void testCreateAndRead() throws Exception {
        database.put("key-1", "value-1");
        assertEquals("value-1", database.get("key-1"));
    }

    @Test
    @Order(2)
    public void testCreateAndDelete() throws Exception {
        database.put("key-2", "value-2");
        assertEquals("value-2", database.get("key-2"));
        database.delete("key-2");
        assertNull(database.get("key-2"), "Deleted key should return null");
    }

    @Test
    @Order(3)
    public void testCreateSameKeyAgain() throws Exception {
        database.put("key-3", "value-3");
        database.put("key-3", "updated-value");
        assertEquals("updated-value", database.get("key-3"));
    }

    @Test
    @Order(4)
    public void testReadUnavailableKey() throws Exception {
        assertNull(database.get("missing-key"), "Non-existent key should return null");
    }

    // ---------- LARGE DATA TESTS ---------- //

    @Test
    @Order(6)
    public void testLargeKey() {
        String key = "k".repeat(300); // If key size limit exists
        assertDoesNotThrow(() -> database.put(key, "someValue"));
    }

    @Test
    @Order(7)
    public void testLargeValue() throws Exception{
        char[] value = new char[1024 * 100]; // 100KB
        String largeValue = new String(value);
        assertDoesNotThrow(() -> database.put("largeValueKey", largeValue));
        assertEquals(largeValue, database.get("largeValueKey"));
    }

    // ---------- COMPACTION TEST ---------- //

    @Test
    @Order(8)
    public void testCompaction() throws Exception {
        // Fill DB with dummy data to force SSTable writes
        for (int i = 0; i < 50; i++) {
            database.put("key-" + i, "value-" + i);
        }

        // Delete some random keys
        for (int i = 10; i < 20; i++) {
            database.delete("key-" + i);
        }

        // Manually trigger compaction if required
        database.forceCompaction(); // If your LSMDatabase supports this
        assertEquals("value-25", database.get("key-25"));
    }

    // ---------- PERFORMANCE TEST ---------- //
    @Test
    @Order(9)
    public void testBulkInsertPerformance() throws Exception {
        Random random = new Random();
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10_000; i++) {
            String key = "perf-" + i;
            String value = "val-" + random.nextInt(1_000_000);
            database.put(key, value);
        }
        long end = System.currentTimeMillis();
        System.out.println("Inserted 10,000 keys in " + (end - start) + "ms");
        assertTrue(end - start < 10_000, "Insertion took too long!");
    }
}
