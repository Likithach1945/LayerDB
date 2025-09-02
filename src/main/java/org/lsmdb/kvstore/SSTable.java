
package org.lsmdb.kvstore;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.GZIPOutputStream;
import java.util.zip.GZIPInputStream;

/**
 * SSTable persistence with tombstone-aware compaction and Bloom filter support.
 */
public class SSTable {

    private static final ReentrantReadWriteLock LOCK = new ReentrantReadWriteLock();

    // Bloom filters per filename (in-memory)
    private static final Map<String, BloomFilter> bloomFilters = new HashMap<>();
    private static final double DEFAULT_FALSE_POSITIVE = 0.01;

    /**
     * Write memtable mapping to disk atomically as sstable_<counter>.txt
     * Values that are null are written as TOMBSTONE marker.
     */
    public static void writeToDisk(String dir, Map<String, String> map, int counter) throws IOException {
        LOCK.writeLock().lock();
        try {
            File folder = new File(dir);
            if (!folder.exists() && !folder.mkdirs()) {
                throw new IOException("Unable to create directory: " + dir);
            }

            // ✅ Generate unique filenames to avoid clashes
            String uniqueId = UUID.randomUUID().toString().replace("-", "");
            String tmpName = "sstable_" + counter + "_" + System.nanoTime() + "_" + uniqueId + ".tmp";
            File tmp = new File(dir, tmpName);
            File finalFile = new File(dir, "sstable_" + counter + ".txt");

            // ✅ Write data safely
            try (FileOutputStream fos = new FileOutputStream(tmp);
                 OutputStreamWriter osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
                 BufferedWriter writer = new BufferedWriter(osw)) {

                for (Map.Entry<String, String> e : map.entrySet()) {
                    String v = e.getValue();
                    if (v == null) v = KVConstants.TOMBSTONE;
                    writer.write(e.getKey() + "=" + v);
                    writer.newLine();
                }
                writer.flush();

                // Best-effort fsync for durability
                try {
                    fos.getFD().sync();
                } catch (IOException syncEx) {
                    System.err.println("[SSTable.writeToDisk] fsync failed: " + syncEx.getMessage());
                }
            }

            // ✅ Ensure temp file exists before moving
            if (!tmp.exists()) {
                throw new IOException("Temp SSTable missing before move: " + tmp.getAbsolutePath());
            }

            // ✅ Retry moving up to 3 times in case of Windows locking
            boolean moved = false;
            for (int i = 0; i < 3 && !moved; i++) {
                try {
                    Files.move(tmp.toPath(), finalFile.toPath(),
                            StandardCopyOption.REPLACE_EXISTING,
                            StandardCopyOption.ATOMIC_MOVE);
                    moved = true;
                } catch (IOException ex) {
                    if (i == 2) { // last attempt failed
                        tmp.delete();
                        throw new IOException("Failed to move tmp SSTable to final file after retries", ex);
                    }
                    try {
                        Thread.sleep(50); // wait before retry
                    } catch (InterruptedException ignored) {}
                }
            }

            // ✅ Build and store bloom filter for this SSTable
            BloomFilter bf = new BloomFilter(Math.max(1, map.size()), DEFAULT_FALSE_POSITIVE);
            for (String k : map.keySet()) bf.add(k);
            bloomFilters.put(finalFile.getName(), bf);

        } finally {
            LOCK.writeLock().unlock();
        }
    }


    /**
     * Lookup uncompressed SSTables (.txt) newest first. Uses Bloom filter to skip files.
     */
    public static String lookup(String dir, String key) throws IOException {
        LOCK.readLock().lock();
        try {
            File folder = new File(dir);
            File[] files = folder.listFiles((f, name) -> name.startsWith("sstable_") && name.endsWith(".txt"));
            if (files == null || files.length == 0) return null;

            Arrays.sort(files, Comparator.comparingLong(File::lastModified).reversed());

            for (File file : files) {
                BloomFilter bf = bloomFilters.get(file.getName());
                if (bf != null && !bf.mightContain(key)) continue;

                try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] parts = line.split("=", 2);
                        if (parts.length != 2) continue;
                        if (parts[0].equals(key)) {
                            String value = parts[1];
                            if (KVConstants.TOMBSTONE.equals(value)) return KVConstants.TOMBSTONE;
                            return value;
                        }
                    }
                } catch (IOException ex) {
                    System.err.println("[lookup] Error reading " + file.getName() + ": " + ex.getMessage());
                }
            }
            return null;
        } finally {
            LOCK.readLock().unlock();
        }
    }

    /**
     * Lookup compressed SSTables (.gz)
     */
    public static String lookupCompressed(String dir, String key) throws IOException {
        LOCK.readLock().lock();
        try {
            File folder = new File(dir);
            File[] files = folder.listFiles((f, name) -> name.startsWith("sstable_") && name.endsWith(".gz"));
            if (files == null || files.length == 0) return null;

            Arrays.sort(files, Comparator.comparingLong(File::lastModified).reversed());

            for (File file : files) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file)), "UTF-8"))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] parts = line.split("=", 2);
                        if (parts.length != 2) continue;
                        if (parts[0].equals(key)) {
                            String value = parts[1];
                            if (KVConstants.TOMBSTONE.equals(value)) return KVConstants.TOMBSTONE;
                            return value;
                        }
                    }
                } catch (IOException ex) {
                    System.err.println("[lookupCompressed] Error reading " + file.getName() + ": " + ex.getMessage());
                }
            }
            return null;
        } finally {
            LOCK.readLock().unlock();
        }
    }

    // Compaction: merge SSTables, newest entries win, skip tombstones when writing final compacted output
    public static void compactBySize(String dir, long sizeLimitBytes) throws IOException {
        LOCK.writeLock().lock();
        try {
            File folder = new File(dir);
            File[] files = folder.listFiles((f, name) -> name.startsWith("sstable_") && (name.endsWith(".txt") || name.endsWith(".gz")));
            if (files == null || files.length == 0) return;

            Arrays.sort(files, Comparator.comparingLong(File::lastModified));
            long total = Arrays.stream(files).mapToLong(File::length).sum();
            if (total <= sizeLimitBytes && sizeLimitBytes > 0) return;

            performCompactionInternal(dir, files);
        } finally {
            LOCK.writeLock().unlock();
        }
    }

    public static void compactByFileCount(String dir, int maxFiles) throws IOException {
        LOCK.writeLock().lock();
        try {
            File folder = new File(dir);
            File[] files = folder.listFiles((f, name) -> name.startsWith("sstable_") && (name.endsWith(".txt") || name.endsWith(".gz")));
            if (files == null || files.length <= maxFiles) return;

            Arrays.sort(files, Comparator.comparingLong(File::lastModified));
            performCompactionInternal(dir, files);
        } finally {
            LOCK.writeLock().unlock();
        }
    }

    private static void performCompactionInternal(String dir, File[] files) throws IOException {
        TreeMap<String, String> merged = new TreeMap<>();
        for (File f : files) {
            if (!f.exists()) continue;
            try (BufferedReader reader = createReader(f)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("=", 2);
                    if (parts.length != 2) continue;
                    merged.put(parts[0], parts[1]); // newest wins because files sorted oldest->newest
                }
            } catch (IOException e) {
                System.err.println("[compact] skipping " + f.getName() + ": " + e.getMessage());
            }
        }

        // Write compacted gz, but skip keys whose latest value is TOMBSTONE
        File temp = new File(dir, "sstable_compacted_temp.gz");
        File finalGz = new File(dir, "sstable_compacted.txt.gz");

        try (GZIPOutputStream gzOut = new GZIPOutputStream(new FileOutputStream(temp));
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(gzOut, "UTF-8"))) {
            for (Map.Entry<String, String> e : merged.entrySet()) {
                String v = e.getValue();
                if (KVConstants.TOMBSTONE.equals(v)) {
                    // tombstone: skip writing to final compacted file (removes key)
                    continue;
                }
                writer.write(e.getKey() + "=" + v);
                writer.newLine();
            }
            writer.flush();
        }

        // atomic rename
        try {
            Files.move(temp.toPath(), finalGz.toPath(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException ex) {
            if (!temp.renameTo(finalGz)) {
                temp.delete();
                throw new IOException("Failed to move compacted file", ex);
            }
        }

        // delete old files and remove bloom entries
        for (File f : files) {
            if (!f.equals(finalGz) && f.exists()) {
                if (!f.delete()) System.err.println("[compact] could not delete " + f.getName());
                bloomFilters.remove(f.getName());
            }
        }

        // build bloom for compacted file (optional - useful)
        buildAndStoreBloomForFile(finalGz);
    }

    private static void buildAndStoreBloomForFile(File f) {
        try {
            BloomFilter bf = new BloomFilter(1024, DEFAULT_FALSE_POSITIVE);
            try (BufferedReader reader = createReader(f)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("=", 2);
                    if (parts.length == 2) bf.add(parts[0]);
                }
            }
            bloomFilters.put(f.getName(), bf);
        } catch (Exception e) {
            System.err.println("[buildBloomForFile] failed for " + f.getName() + ": " + e.getMessage());
        }
    }

    private static BufferedReader createReader(File file) throws IOException {
        if (file.getName().endsWith(".gz")) {
            return new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file)), "UTF-8"));
        } else {
            return new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));
        }
    }

    // Expose a method to build a per-file lightweight bloom (used by LSMDatabase)
    public static SSTableBloom buildBloomFilter(File sstableFile) {
        SSTableBloom bloom = new SSTableBloom(8 * 1024);
        try (BufferedReader reader = createReader(sstableFile)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("=", 2);
                if (parts.length == 2) bloom.add(parts[0]);
            }
        } catch (Exception e) {
            System.err.println("[buildBloomFilter] " + e.getMessage());
        }
        return bloom;
    }

//     ==================== COMPRESSION STATS ====================
    public static class CompressionStats {
        private final int totalFiles;
        private final int compressedFiles;
        private final long totalSize;
        private final long compressedSize;

        public CompressionStats(int totalFiles, int compressedFiles, long totalSize, long compressedSize) {
            this.totalFiles = totalFiles;
            this.compressedFiles = compressedFiles;
            this.totalSize = totalSize;
            this.compressedSize = compressedSize;
        }

        public int getTotalFiles() { return totalFiles; }
        public int getCompressedFiles() { return compressedFiles; }
        public long getTotalSize() { return totalSize; }
        public long getCompressedSize() { return compressedSize; }
        public double getCompressionRatio() { return totalSize > 0 ? (double) compressedSize / totalSize * 100 : 0; }

        @Override
        public String toString() {
            return String.format(
                    "CompressionStats{files=%d/%d compressed, size=%.1f KB/%.1f KB (%.1f%% compression)}",
                    compressedFiles, totalFiles,
                    compressedSize / 1024.0, totalSize / 1024.0, getCompressionRatio()
            );
        }
    }

    public static CompressionStats getCompressionStats(String dir) throws IOException {
        File folder = new File(dir);
        File[] files = folder.listFiles((f, name) -> name.startsWith("sstable_"));
        if (files == null || files.length == 0) return new CompressionStats(0, 0, 0, 0);

        long totalSize = 0, compressedSize = 0;
        int compressedCount = 0, total = files.length;

        for (File f : files) {
            long sz = f.length();
            totalSize += sz;
            if (f.getName().endsWith(".gz")) {
                compressedSize += sz;
                compressedCount++;
            }
        }

        return new CompressionStats(total, compressedCount, totalSize, compressedSize);
    }

}
