package org.lsmdb.kvstore;

import org.junit.jupiter.api.*;
import java.io.File;
import java.sql.*;
import java.util.UUID;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class PerformanceComparisionWithSQLite {

    private static final String TEST_DIR = "DBFiles/lsm_performance_test";
    private static LSMDatabase lsmDatabase;
    private static Connection sqliteConn;



    @BeforeEach
    public void clearSQLite() throws Exception {
        try (Statement stmt = sqliteConn.createStatement()) {
            stmt.execute("DELETE FROM keystore");
        }
    }


    @BeforeAll
    public static void setUp() throws Exception {
        // Create fresh test directory for LSM DB
        File dir = new File(TEST_DIR);
        if (dir.exists()) {
            for (File file : dir.listFiles()) file.delete();
        } else {
            dir.mkdirs();
        }

        // Initialize LSMDatabase
        lsmDatabase = new LSMDatabase(TEST_DIR);

        // ✅ Load SQLite JDBC driver manually
        Class.forName("org.sqlite.JDBC");

        // ✅ Setup SQLite connection
        String url = "jdbc:sqlite:DBFiles/sqlite_performance.db";
        sqliteConn = DriverManager.getConnection(url);

        // Create table if it doesn't exist
        try (Statement stmt = sqliteConn.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS keystore (key TEXT PRIMARY KEY, value TEXT);");
        }
    }


    @AfterAll
    public static void tearDown() throws Exception {
        if (sqliteConn != null && !sqliteConn.isClosed()) {
            sqliteConn.close();
        }
    }

    /** Utility: Insert into SQLite */
    private void insertIntoSQLite(String key, String value) throws SQLException {
        String sql = "INSERT OR REPLACE INTO keystore(key, value) VALUES (?, ?)";
        try (PreparedStatement pstmt = sqliteConn.prepareStatement(sql)) {
            pstmt.setString(1, key);
            pstmt.setString(2, value);
            pstmt.executeUpdate();
        }
    }


    /** Utility: Read from SQLite */
    private static String getFromSQLite(String key) throws SQLException {
        String query = "SELECT value FROM keystore WHERE key = ?";
        try (PreparedStatement stmt = sqliteConn.prepareStatement(query)) {
            stmt.setString(1, key);
            ResultSet rs = stmt.executeQuery();
            return rs.next() ? rs.getString("value") : null;
        }
    }

    /** Benchmark for inserting N keys */
    private void benchmarkInsert(int n) throws Exception {
        System.out.println("\n=== Benchmarking INSERT for " + n + " keys ===");

        // LSMDatabase insert benchmark
        long startLSM = System.nanoTime();
        for (int i = 0; i < n; i++) {
            lsmDatabase.put("k" + i, "v" + i);
        }
        long endLSM = System.nanoTime();
        System.out.println("LSMDatabase INSERT " + n + " keys took: " +
                (endLSM - startLSM)/1e6 + " ms");

        // SQLite insert benchmark
        long startSQL = System.nanoTime();
        for (int i = 0; i < n; i++) {
            insertIntoSQLite("k" + i, "v" + i);
        }
        long endSQL = System.nanoTime();
        System.out.println("SQLite INSERT " + n + " keys took: " +
                (endSQL - startSQL)/1e6 + " ms");
    }

    /** Benchmark for reading N keys */
    private void benchmarkRead(int n) throws Exception {
        System.out.println("\n=== Benchmarking READ for " + n + " keys ===");

        // LSMDatabase read benchmark
        long startLSM = System.nanoTime();
        for (int i = 0; i < n; i++) {
            lsmDatabase.get("k" + i);
        }
        long endLSM = System.nanoTime();
        System.out.println("LSMDatabase READ " + n + " keys took: " +
                (endLSM - startLSM)/1e6 + " ms");

        // SQLite read benchmark
        long startSQL = System.nanoTime();
        for (int i = 0; i < n; i++) {
            getFromSQLite("k" + i);
        }
        long endSQL = System.nanoTime();
        System.out.println("SQLite READ " + n + " keys took: " +
                (endSQL - startSQL)/1e6 + " ms");
    }

    @Test @Order(1)
    public void testInsertPerformance() throws Exception {
        benchmarkInsert(100);
        benchmarkInsert(1000);
        benchmarkInsert(5000);
        benchmarkInsert(10000);
    }

    @Test @Order(2)
    public void testReadPerformance() throws Exception {
        benchmarkRead(100);
        benchmarkRead(1000);
        benchmarkRead(5000);
        benchmarkRead(10000);
    }
}
