//package org.lsmdb.kvstore;
//
//import java.io.FileWriter;
//import java.io.IOException;
//
//public class WALog {
//
//    private FileWriter writer;
//    private final String filePath;
//
//    public WALog(String path) throws IOException {
//        this.filePath = path;
//        writer = new FileWriter(path, true);
//    }
//
//    public void append(String key, String value) throws IOException {
//        writer.write(key + "=" + (value != null ? value : "null") + "\n");
//        writer.flush();
//    }
//
//
//    public void clear() throws IOException {
//        writer.close();
//        writer = new FileWriter(filePath, false);
//    }
//}


package org.lsmdb.kvstore;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * Simple WAL (write-ahead log) with replay support.
 */
public class WALog {

    private BufferedWriter writer;
    private final String filePath;

    public WALog(String path) throws IOException {
        this.filePath = path;
        openWriter(true);
    }

    private synchronized void openWriter(boolean append) throws IOException {
        if (writer != null) {
            try { writer.close(); } catch (IOException ignored) {}
        }
        writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filePath, append), StandardCharsets.UTF_8));
    }

    /**
     * Append a key/value to WAL. If value is null, write tombstone marker.
     */
    public synchronized void append(String key, String value) throws IOException {
        String toWrite = key + "=" + (value != null ? value : KVConstants.TOMBSTONE) + "\n";
        writer.write(toWrite);
        writer.flush(); // ensure durability
    }

    /**
     * Clear the WAL by truncating the file.
     * Keeps writer open for future appends.
     */
    public synchronized void clear() throws IOException {
        try {
            writer.close();
        } catch (IOException ignored) {}
        // Truncate file by opening FileOutputStream without append
        try (FileOutputStream fos = new FileOutputStream(filePath, false)) {
            // nothing, truncates file
        }
        // reopen writer in append mode
        openWriter(true);
    }

    /**
     * Replay WAL into the provided MemTable.
     * Each line is 'key=value' where value could be TOMBSTONE.
     */
    public synchronized void replayInto(MemTable mem) throws IOException {
        File f = new File(filePath);
        if (!f.exists()) return;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(f), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("=", 2);
                if (parts.length == 2) {
                    String key = parts[0];
                    String val = parts[1];
                    if (KVConstants.TOMBSTONE.equals(val)) {
                        mem.put(key, null); // tombstone stored as null in memtable
                    } else {
                        mem.put(key, val);
                    }
                }
            }
        }
    }

    /**
     * Close writer (for shutdown).
     */
//    public synchronized void close() {
//        try { if (writer != null) writer.close(); } catch (IOException ignored) {}
//        writer = null;
//    }

    public void close() throws IOException {
        if (writer != null) {
            writer.flush();
            writer.close();
        }
    }

}
