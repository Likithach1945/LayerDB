
package org.lsmdb.kvstore;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 * Thread-safe in-memory key-value store used by LSMTree.
 * Uses a TreeMap for sorted key storage.
 */
public class MemTable {

    private final TreeMap<String, String> map = new TreeMap<>();
    private int currentSize = 0;

    /**
     * Inserts or updates a key-value pair.
     * Thread-safe to avoid concurrent modification issues.
     */
    public synchronized void put(String key, String value) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        // Calculate size difference
        int delta = key.length() + (value != null ? value.length() : 0);

        // If updating existing value, adjust currentSize accordingly
        if (map.containsKey(key)) {
            String oldValue = map.get(key);
            currentSize -= (key.length() + (oldValue != null ? oldValue.length() : 0));
        }

        // Insert or update
        map.put(key, value);
        currentSize += delta;

        // Debug logging
        // System.out.println("[MemTable] PUT key=" + key + ", size=" + formatSize(currentSize));
    }

    /**
     * Retrieves a value for the given key.
     */
    public synchronized String get(String key) {
        return map.get(key);
    }

    /**
     * Returns an immutable snapshot of the current MemTable data.
     */
    public synchronized Map<String, String> dump() {
        return Collections.unmodifiableMap(new TreeMap<>(map));
    }

    /**
     * Returns the total memory usage of the MemTable in bytes.
     */
    public synchronized int sizeInBytes() {
        return currentSize;
    }

    /**
     * Clears the MemTable data completely.
     */
    public synchronized void clear() {
        map.clear();
        currentSize = 0;
    }

    /**
     * Returns whether the MemTable is empty.
     */
    public synchronized boolean isEmpty() {
        return map.isEmpty();
    }

    /**
     * Returns the number of key-value entries.
     */
    public synchronized int size() {
        return map.size();
    }

    /**
     * Utility to format size for debugging.
     */
    private String formatSize(int bytes) {
        if (bytes < 1024) return bytes + "B";
        if (bytes < 1024 * 1024) return String.format("%.2fKB", bytes / 1024.0);
        return String.format("%.2fMB", bytes / (1024.0 * 1024.0));
    }
}
