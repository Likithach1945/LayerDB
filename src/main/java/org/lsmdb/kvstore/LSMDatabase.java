//
//package org.lsmdb.kvstore;
//
//import java.io.*;
//import java.util.Arrays;
//import java.util.List;
//import java.util.concurrent.CopyOnWriteArrayList;
//import java.util.concurrent.atomic.AtomicBoolean;
//import java.util.concurrent.locks.ReentrantLock;
//import java.util.zip.GZIPInputStream;
//
//public class LSMDatabase {
//
//    private static final int MEMTABLE_LIMIT_BYTES = 4 * 1024; // 4KB
//    private static final long SSTABLE_DISK_LIMIT = 1024L * 1024L * 1024L; // 1GB
//    private static final int MAX_SSTABLE_FILES = 10;
//
//    private volatile MemTable activeMemTable;
//    private final List<MemTable> immutableMemTables = new CopyOnWriteArrayList<>();
//    private final WALog wal;
//    private final String dataDir;
//    private int sstableCounter = 0;
//    private final AtomicBoolean flusherRunning = new AtomicBoolean(false);
//    private BloomFilter bloomFilter;
//
//    private final ReentrantLock compactionLock = new ReentrantLock();
//    private final List<SSTableBloom> sstableBloomFilters = new CopyOnWriteArrayList<>();
//
//    public LSMDatabase(String dataDir) throws Exception {
//        this.dataDir = dataDir;
//        new File(dataDir).mkdirs();
//        this.activeMemTable = new MemTable();
//        this.wal = new WALog(dataDir + "/wal.log");
//
//
//        // Initialize Bloom filter (size and false positive rate are configurable)
//        bloomFilter = new BloomFilter(10000, 0.01); // example: capacity=10k, false positive rate=1%
//        rebuildBloomFilterFromDisk();
//    }
//
//    public void put(String key, String value) throws Exception {
//        wal.append(key, value);
//
//        synchronized (this) {
//            activeMemTable.put(key, value);
//            bloomFilter.add(key);
//
//            if (activeMemTable.sizeInBytes() >= MEMTABLE_LIMIT_BYTES) {
//                MemTable toFlush = activeMemTable;
//                immutableMemTables.add(toFlush);
//                activeMemTable = new MemTable();
//                triggerBackgroundFlush();
//            }
//        }
//    }
//
//    public String get(String key) throws Exception {
//        String val = activeMemTable.get(key);
//        if (val != null) return val;
//
//        for (int i = immutableMemTables.size() - 1; i >= 0; i--) {
//            val = immutableMemTables.get(i).get(key);
//            if (val != null) return val;
//        }
//
//        // Use Bloom filter to check SSTables
//        if (mightContainInSSTables(key)) {
//            String result = SSTable.lookup(dataDir, key);
//            if (result != null) return result;
//
//            result = SSTable.lookupCompressed(dataDir, key);
//            if (result != null) return result;
//        }
//
//        return null;
//    }
//
//    public boolean mightContainInSSTables(String key) {
//        for (SSTableBloom bf : sstableBloomFilters) {
//            if (bf.mightContain(key)) return true;
//        }
//        return false;
//    }
//
//    private void triggerBackgroundFlush() {
//        if (flusherRunning.compareAndSet(false, true)) {
//            Thread flusher = new Thread(() -> {
//                try {
//                    while (true) {
//                        MemTable toFlush;
//                        synchronized (immutableMemTables) {
//                            if (immutableMemTables.isEmpty()) break;
//                            toFlush = immutableMemTables.remove(0); // atomic removal
//                        }
//
//                        try {
//                            System.out.println("[Flusher] Writing SSTable #" + sstableCounter);
//                            SSTable.writeToDisk(dataDir, toFlush.dump(), sstableCounter);
//                            File sstableFile = new File(dataDir, "sstable_" + sstableCounter + ".txt");
//                            SSTableBloom bf = SSTable.buildBloomFilter(sstableFile);
//                            sstableBloomFilters.add(bf);
//                            sstableCounter++;
//                        } catch (Exception writeEx) {
////                            System.err.println("[Flusher] Failed to write SSTable: " + writeEx.getMessage());
//                            writeEx.printStackTrace();
//                            break;
//                        }
//
//                        if (immutableMemTables.isEmpty()) {
//                            try { wal.clear(); } catch (Exception e) { }
//                        }
//
//                        try {
//                            maybeCompactBySize();
//                            maybeCompactByFileCount();
//                        } catch (Exception compEx) { }
//                    }
//                } finally {
//                    flusherRunning.set(false);
//                    if (!immutableMemTables.isEmpty()) triggerBackgroundFlush();
//                }
//            }, "LSM-Flusher-" + System.nanoTime());
//
//            flusher.setDaemon(true);
//            flusher.start();
//        }
//    }
//
//    public synchronized void flush() throws Exception {
//        if (activeMemTable == null || activeMemTable.sizeInBytes() == 0) {
////            System.out.println("[flush] Skipping flush: MemTable is empty");
//            return;
//        }
//
//        immutableMemTables.add(activeMemTable);
//        activeMemTable = new MemTable();
//
//        while (true) {
//            MemTable toFlush;
//            synchronized (immutableMemTables) {
//                if (immutableMemTables.isEmpty()) break;
//                toFlush = immutableMemTables.remove(0); // atomic removal
//            }
//
//            SSTable.writeToDisk(dataDir, toFlush.dump(), sstableCounter);
//            File sstableFile = new File(dataDir, "sstable_" + sstableCounter + ".txt");
//            SSTableBloom bf = SSTable.buildBloomFilter(sstableFile);
//            sstableBloomFilters.add(bf);
//            sstableCounter++;
//
//            if (immutableMemTables.isEmpty()) {
//                try { wal.clear(); } catch (Exception e) { }
//            }
//
//            maybeCompactBySize();
//            maybeCompactByFileCount();
//        }
//    }
//
//    private void maybeCompactBySize() throws Exception {
//        File dir = new File(dataDir);
//        File[] sstableFiles = dir.listFiles((f, name) ->
//                name.startsWith("sstable_") && !name.contains("compacted") && !name.endsWith(".tmp"));
//
//        if (sstableFiles == null || sstableFiles.length == 0) return;
//
//        long totalSize = Arrays.stream(sstableFiles).mapToLong(File::length).sum();
//        if (totalSize > SSTABLE_DISK_LIMIT) {
//            if (!compactionLock.tryLock()) return;
//            try {
//                SSTable.compactBySize(dataDir, SSTABLE_DISK_LIMIT);
////                rebuildBloomFilterFromDisk();
//            } finally {
//                compactionLock.unlock();
//            }
//        }
//    }
//
//    private void maybeCompactByFileCount() throws Exception {
//        File dir = new File(dataDir);
//        File[] sstableFiles = dir.listFiles((f, name) ->
//                name.startsWith("sstable_") && !name.contains("compacted") && !name.endsWith(".tmp"));
//
//        if (sstableFiles == null || sstableFiles.length <= MAX_SSTABLE_FILES) return;
//
//        if (!compactionLock.tryLock()) return;
//        try {
//            SSTable.compactByFileCount(dataDir, MAX_SSTABLE_FILES);
////            rebuildBloomFilterFromDisk();
//        } finally {
//            compactionLock.unlock();
//        }
//    }
//
//    public void forceCompaction() throws Exception {
//        System.out.println("Forcing compaction...");
//        compactionLock.lock();
//        try {
//            SSTable.compactBySize(dataDir, 0); // force all
//            SSTable.compactByFileCount(dataDir, 0);
//        } finally {
//            rebuildBloomFilterFromDisk();
//            compactionLock.unlock();
//        }
//    }
//
//    public void rebuildBloomFilterFromDisk() throws Exception {
//        bloomFilter = new BloomFilter(10000, 0.01); // reset
//        sstableBloomFilters.clear();
//
//        File[] files = new File(dataDir).listFiles((f, name) ->
//                name.startsWith("sstable_") || name.contains("compacted" )
//        );
//        if (files == null) return;
//
//        for (File f : files) {
//            if (!f.getName().endsWith(".txt") && !f.getName().endsWith(".txt.gz")) continue;
//
//            // Rebuild main Bloom filter
//            try (BufferedReader reader = f.getName().endsWith(".gz") ?
//                    new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(f)))) :
//                    new BufferedReader(new FileReader(f))) {
//
//                String line;
//                while ((line = reader.readLine()) != null) {
//                    String[] parts = line.split("=", 2);
//                    if (parts.length == 2) bloomFilter.add(parts[0]);
//                }
//            }
//
//            // Rebuild SSTableBloom for each SSTable
//            SSTableBloom bf = SSTable.buildBloomFilter(f);
//            sstableBloomFilters.add(bf);
//        }
//    }
//
//
//
//    // Wait until all pending flushes complete
//    public void waitForFlushCompletion() throws InterruptedException {
//        while (flusherRunning.get() || !immutableMemTables.isEmpty()) {
//            Thread.sleep(10); // small sleep to avoid busy-wait
//        }
//    }
//
//
//
//
//
//    public DatabaseStats getStats() throws Exception {
//        File dir = new File(dataDir);
//
//        File[] sstableFiles = dir.listFiles((f, name) ->
//                name.startsWith("sstable_") &&
//                        !name.equals("sstable_compacted.txt") &&
//                        (name.endsWith(".txt") || name.endsWith(".gz"))
//        );
//
//        long totalSize = 0L;
//        int fileCount = 0;
//
//        if (sstableFiles != null) {
//            fileCount = sstableFiles.length;
//            totalSize = Arrays.stream(sstableFiles).mapToLong(File::length).sum();
//        }
//
//        return new DatabaseStats(
//                activeMemTable.sizeInBytes(),
//                immutableMemTables.size(),
//                totalSize,
//                fileCount,
//                SSTABLE_DISK_LIMIT,
//                MEMTABLE_LIMIT_BYTES
//        );
//    }
//
//    public static class DatabaseStats {
//        private final int activeMemtableSize;
//        private final int immutableMemtableCount;
//        private final long totalSSTableSize;
//        private final int sstableFileCount;
//        private final long sstableLimit;
//        private final int memtableLimit;
//
//        public DatabaseStats(int activeMemtableSize, int immutableMemtableCount, long totalSSTableSize,
//                             int sstableFileCount, long sstableLimit, int memtableLimit) {
//            this.activeMemtableSize = activeMemtableSize;
//            this.immutableMemtableCount = immutableMemtableCount;
//            this.totalSSTableSize = totalSSTableSize;
//            this.sstableFileCount = sstableFileCount;
//            this.sstableLimit = sstableLimit;
//            this.memtableLimit = memtableLimit;
//        }
//
//        public int getActiveMemtableSize() { return activeMemtableSize; }
//        public int getImmutableMemtableCount() { return immutableMemtableCount; }
//        public long getTotalSSTableSize() { return totalSSTableSize; }
//        public int getSstableFileCount() { return sstableFileCount; }
//        public long getSSTableLimit() { return sstableLimit; }
//        public int getMemtableLimit() { return memtableLimit; }
//
//        public double getActiveMemtableUsagePercent() {
//            return (double) activeMemtableSize / memtableLimit * 100.0;
//        }
//
//        public double getSSTableUsagePercent() {
//            return (double) totalSSTableSize / sstableLimit * 100.0;
//        }
//
//        @Override
//        public String toString() {
//            return String.format(
//                    "DatabaseStats{active_memtable=%d/%d bytes (%.1f%%), immutables=%d, " +
//                            "sstables=%d files, %.2f MB/%.2f GB (%.2f%% used)}",
//                    activeMemtableSize,
//                    memtableLimit,
//                    getActiveMemtableUsagePercent(),
//                    immutableMemtableCount,
//                    sstableFileCount,
//                    totalSSTableSize / (1024.0 * 1024.0),
//                    sstableLimit / (1024.0 * 1024.0 * 1024.0),
//                    getSSTableUsagePercent()
//            );
//        }
//    }
//}
//


package org.lsmdb.kvstore;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.GZIPInputStream;

/**
 * LSMDatabase with tombstone support, WAL replay, and safe compaction.
 * Preserves existing public methods and adds delete(key).
 */
public class LSMDatabase {

    private static final int MEMTABLE_LIMIT_BYTES = 4 * 1024; // 4KB
    private static final long SSTABLE_DISK_LIMIT = 1024L * 1024L * 1024L; // 1GB
    private static final int MAX_SSTABLE_FILES = 10;

    private volatile MemTable activeMemTable;
    private final List<MemTable> immutableMemTables = new CopyOnWriteArrayList<>();
    private final WALog wal;
    private final String dataDir;
    private int sstableCounter = 0;
    private final AtomicBoolean flusherRunning = new AtomicBoolean(false);
    private BloomFilter bloomFilter;

    private final ReentrantLock compactionLock = new ReentrantLock();
    private final List<SSTableBloom> sstableBloomFilters = new CopyOnWriteArrayList<>();

    public LSMDatabase(String dataDir) throws Exception {
        this.dataDir = dataDir;
        new File(dataDir).mkdirs();
        this.activeMemTable = new MemTable();
        this.wal = new WALog(dataDir + "/wal.log");

        // set sstableCounter to next available index based on existing files
        this.sstableCounter = computeNextSSTableCounter();

        // Initialize Bloom filter (size and false positive rate are configurable)
        bloomFilter = new BloomFilter(10000, 0.01); // example: capacity=10k, false positive rate=1%

        // Recover memtable from WAL (durability)
        wal.replayInto(activeMemTable);

        // Rebuild bloom filters from existing sstables
        rebuildBloomFilterFromDisk();
    }

    private int computeNextSSTableCounter() {
        File dir = new File(dataDir);
        File[] files = dir.listFiles((f, name) -> name.startsWith("sstable_") && name.endsWith(".txt"));
        if (files == null || files.length == 0) return 0;
        int max = -1;
        for (File f : files) {
            String nm = f.getName(); // sstable_12.txt
            String idx = nm.replace("sstable_", "").replace(".txt", "");
            try {
                int v = Integer.parseInt(idx);
                if (v > max) max = v;
            } catch (NumberFormatException ignored) {}
        }
        return max + 1;
    }

    /**
     * Insert/update a key
     */
    public void put(String key, String value) throws Exception {
        wal.append(key, value);
        synchronized (this) {
            activeMemTable.put(key, value);
            bloomFilter.add(key);

            if (activeMemTable.sizeInBytes() >= MEMTABLE_LIMIT_BYTES) {
                MemTable toFlush = activeMemTable;
                immutableMemTables.add(toFlush);
                activeMemTable = new MemTable();
                triggerBackgroundFlush();
            }
        }
    }

    /**
     * Delete (tombstone) a key.
     */
    public void delete(String key) throws Exception {
        wal.append(key, null); // null means tombstone in WAL
        synchronized (this) {
            activeMemTable.put(key, null); // store tombstone (null) in memtable
            bloomFilter.add(key); // still add so we check SSTables later

            if (activeMemTable.sizeInBytes() >= MEMTABLE_LIMIT_BYTES) {
                MemTable toFlush = activeMemTable;
                immutableMemTables.add(toFlush);
                activeMemTable = new MemTable();
                triggerBackgroundFlush();
            }
        }
    }

    /**
     * Get value for key. Returns null if not found or tombstoned.
     */
    public String get(String key) throws Exception {
        // 1. Active memtable
        String val = activeMemTable.get(key);
        if (val != null) {
            if (val == null) return null; // defensive, though get() returns null for missing keys
            if (KVConstants.TOMBSTONE.equals(val)) return null;
            return val;
        }

        // 2. Immutable memtables newest first
        for (int i = immutableMemTables.size() - 1; i >= 0; i--) {
            val = immutableMemTables.get(i).get(key);
            if (val != null) {
                if (KVConstants.TOMBSTONE.equals(val)) return null;
                return val;
            }
        }

        // 3. Check uncompressed SSTables then compressed
        // Use per-file SSTableBloom first to avoid scanning if possible
        if (mightContainInSSTables(key)) {
            String result = SSTable.lookup(dataDir, key);
            if (KVConstants.TOMBSTONE.equals(result)) return null;
            if (result != null) return result;

            result = SSTable.lookupCompressed(dataDir, key);
            if (KVConstants.TOMBSTONE.equals(result)) return null;
            if (result != null) return result;
        }

        return null;
    }

    public boolean mightContainInSSTables(String key) {
        for (SSTableBloom bf : sstableBloomFilters) {
            if (bf.mightContain(key)) return true;
        }
        return false;
    }

    // background flush and flush() remain, but ensure tombstones handled by SSTable.writeToDisk
    private synchronized void triggerBackgroundFlush() {
        if (flusherRunning.compareAndSet(false, true)) {
            Thread flusher = new Thread(() -> {
                try {
                    while (true) {
                        MemTable toFlush;
                        synchronized (immutableMemTables) {
                            if (immutableMemTables.isEmpty()) break;
                            toFlush = immutableMemTables.remove(0);
                        }

                        try {
                            System.out.println("[Flusher] Writing SSTable #" + sstableCounter);
                            SSTable.writeToDisk(dataDir, toFlush.dump(), sstableCounter);
                            File sstableFile = new File(dataDir, "sstable_" + sstableCounter + ".txt");
                            SSTableBloom bf = SSTable.buildBloomFilter(sstableFile);
                            sstableBloomFilters.add(bf);
                            sstableCounter++;
                        } catch (Exception writeEx) {
                            writeEx.printStackTrace();
                            break;
                        }

                        if (immutableMemTables.isEmpty()) {
                            try { wal.clear(); } catch (Exception e) { }
                        }

                        try {
                            maybeCompactBySize();
                            maybeCompactByFileCount();
                        } catch (Exception compEx) { }
                    }
                } finally {
                    flusherRunning.set(false);
                    if (!immutableMemTables.isEmpty()) triggerBackgroundFlush();
                }
            }, "LSM-Flusher-" + System.nanoTime());

            flusher.setDaemon(true);
            flusher.start();
        }
    }

    public synchronized void flush() throws Exception {
        if (activeMemTable == null || activeMemTable.sizeInBytes() == 0) {
            return;
        }

        immutableMemTables.add(activeMemTable);
        activeMemTable = new MemTable();

        while (true) {
            MemTable toFlush;
            synchronized (immutableMemTables) {
                if (immutableMemTables.isEmpty()) break;
                toFlush = immutableMemTables.remove(0);
            }

            SSTable.writeToDisk(dataDir, toFlush.dump(), sstableCounter);
            File sstableFile = new File(dataDir, "sstable_" + sstableCounter + ".txt");
            SSTableBloom bf = SSTable.buildBloomFilter(sstableFile);
            sstableBloomFilters.add(bf);
            sstableCounter++;

            if (immutableMemTables.isEmpty()) {
                try { wal.clear(); } catch (Exception e) { }
            }

            maybeCompactBySize();
            maybeCompactByFileCount();
        }
    }

    private void maybeCompactBySize() throws Exception {
        File dir = new File(dataDir);
        File[] sstableFiles = dir.listFiles((f, name) ->
                name.startsWith("sstable_") && !name.contains("compacted") && !name.endsWith(".tmp"));

        if (sstableFiles == null || sstableFiles.length == 0) return;

        long totalSize = Arrays.stream(sstableFiles).mapToLong(File::length).sum();
        if (totalSize > SSTABLE_DISK_LIMIT) {
            if (!compactionLock.tryLock()) return;
            try {
                SSTable.compactBySize(dataDir, SSTABLE_DISK_LIMIT);
            } finally {
                compactionLock.unlock();
            }
        }
    }

    private void maybeCompactByFileCount() throws Exception {
        File dir = new File(dataDir);
        File[] sstableFiles = dir.listFiles((f, name) ->
                name.startsWith("sstable_") && !name.contains("compacted") && !name.endsWith(".tmp"));

        if (sstableFiles == null || sstableFiles.length <= MAX_SSTABLE_FILES) return;

        if (!compactionLock.tryLock()) return;
        try {
            SSTable.compactByFileCount(dataDir, MAX_SSTABLE_FILES);
        } finally {
            compactionLock.unlock();
        }
    }

    public void forceCompaction() throws Exception {
        System.out.println("Forcing compaction...");
        compactionLock.lock();
        try {
            SSTable.compactBySize(dataDir, 0); // force all
            SSTable.compactByFileCount(dataDir, 0);
        } finally {
            rebuildBloomFilterFromDisk();
            compactionLock.unlock();
        }
    }

    public void rebuildBloomFilterFromDisk() throws Exception {
        bloomFilter = new BloomFilter(10000, 0.01); // reset
        sstableBloomFilters.clear();

        File[] files = new File(dataDir).listFiles((f, name) ->
                name.startsWith("sstable_") || name.contains("compacted")
        );
        if (files == null) return;

        for (File f : files) {
            if (!f.getName().endsWith(".txt") && !f.getName().endsWith(".gz")) continue;

            // Rebuild main Bloom filter
            try (BufferedReader reader = f.getName().endsWith(".gz") ?
                    new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(f)), "UTF-8")) :
                    new BufferedReader(new InputStreamReader(new FileInputStream(f), "UTF-8"))) {

                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("=", 2);
                    if (parts.length == 2) bloomFilter.add(parts[0]);
                }
            }catch (IOException e) {
                System.err.println("Failed to read SSTable " + f.getName() + ": " + e.getMessage());
            }
            // Rebuild per-file SSTableBloom
            SSTableBloom bf = SSTable.buildBloomFilter(f);
            sstableBloomFilters.add(bf);
        }
    }

    // Wait until all pending flushes complete
    public void waitForFlushCompletion() throws InterruptedException {
        while (flusherRunning.get() || !immutableMemTables.isEmpty()) {
            Thread.sleep(10); // small sleep to avoid busy-wait
        }
    }

    public void close() throws IOException {
        if (wal != null) {
            wal.close();
        }

    }


    public DatabaseStats getStats() throws Exception {
        File dir = new File(dataDir);

        File[] sstableFiles = dir.listFiles((f, name) ->
                name.startsWith("sstable_") &&
                        !name.equals("sstable_compacted.txt") &&
                        (name.endsWith(".txt") || name.endsWith(".gz"))
        );

        long totalSize = 0L;
        int fileCount = 0;

        if (sstableFiles != null) {
            fileCount = sstableFiles.length;
            totalSize = Arrays.stream(sstableFiles).mapToLong(File::length).sum();
        }

        return new DatabaseStats(
                activeMemTable.sizeInBytes(),
                immutableMemTables.size(),
                totalSize,
                fileCount,
                SSTABLE_DISK_LIMIT,
                MEMTABLE_LIMIT_BYTES
        );
    }

    public static class DatabaseStats {
        private final int activeMemtableSize;
        private final int immutableMemtableCount;
        private final long totalSSTableSize;
        private final int sstableFileCount;
        private final long sstableLimit;
        private final int memtableLimit;

        public DatabaseStats(int activeMemtableSize, int immutableMemtableCount, long totalSSTableSize,
                             int sstableFileCount, long sstableLimit, int memtableLimit) {
            this.activeMemtableSize = activeMemtableSize;
            this.immutableMemtableCount = immutableMemtableCount;
            this.totalSSTableSize = totalSSTableSize;
            this.sstableFileCount = sstableFileCount;
            this.sstableLimit = sstableLimit;
            this.memtableLimit = memtableLimit;
        }

        public int getActiveMemtableSize() { return activeMemtableSize; }
        public int getImmutableMemtableCount() { return immutableMemtableCount; }
        public long getTotalSSTableSize() { return totalSSTableSize; }
        public int getSstableFileCount() { return sstableFileCount; }
        public long getSSTableLimit() { return sstableLimit; }
        public int getMemtableLimit() { return memtableLimit; }

        public double getActiveMemtableUsagePercent() {
            return (double) activeMemtableSize / memtableLimit * 100.0;
        }

        public double getSSTableUsagePercent() {
            return (double) totalSSTableSize / sstableLimit * 100.0;
        }

        @Override
        public String toString() {
            return String.format(
                    "DatabaseStats{active_memtable=%d/%d bytes (%.1f%%), immutables=%d, " +
                            "sstables=%d files, %.2f MB/%.2f GB (%.2f%% used)}",
                    activeMemtableSize,
                    memtableLimit,
                    getActiveMemtableUsagePercent(),
                    immutableMemtableCount,
                    sstableFileCount,
                    totalSSTableSize / (1024.0 * 1024.0),
                    sstableLimit / (1024.0 * 1024.0 * 1024.0),
                    getSSTableUsagePercent()
            );
        }
    }
}

