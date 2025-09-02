package org.lsmdb.kvstore;

import java.util.BitSet;
import java.nio.charset.StandardCharsets;

/**
 * Simple Bloom filter implementation for LSMDatabase SSTables.
 */
public class BloomFilter {

    private final BitSet bitset;
    private final int size;
    private final int hashFunctions;

    /**
     * @param expectedItems Expected number of items to store
     * @param falsePositiveRate Desired false positive probability (0 < p < 1)
     */
    public BloomFilter(int expectedItems, double falsePositiveRate) {
        if (expectedItems <= 0) throw new IllegalArgumentException("expectedItems must be > 0");
        if (falsePositiveRate <= 0 || falsePositiveRate >= 1) throw new IllegalArgumentException("falsePositiveRate must be between 0 and 1");

        // Optimal size of bitset
        this.size = (int) Math.ceil(-(expectedItems * Math.log(falsePositiveRate)) / (Math.pow(Math.log(2), 2)));
        // Optimal number of hash functions
        this.hashFunctions = (int) Math.round((size / (double) expectedItems) * Math.log(2));

        this.bitset = new BitSet(size);
    }

    /** Add a key to the Bloom filter */
    public void add(String key) {
        int[] hashes = getHashes(key);
        for (int hash : hashes) {
            bitset.set(Math.abs(hash % size));
        }
    }

    /** Check if key might exist */
    public boolean mightContain(String key) {
        int[] hashes = getHashes(key);
        for (int hash : hashes) {
            if (!bitset.get(Math.abs(hash % size))) return false;
        }
        return true;
    }

    public void clear() {
        bitset.clear(); // Clears all bits
    }

    /** Generate multiple hash codes for the key */
    private int[] getHashes(String key) {
        int[] result = new int[hashFunctions];
        byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
        int hash1 = hashCode1(bytes);
        int hash2 = hashCode2(bytes);
        for (int i = 0; i < hashFunctions; i++) {
            result[i] = hash1 + i * hash2;
        }
        return result;
    }

    private int hashCode1(byte[] bytes) {
        int hash = 0;
        for (byte b : bytes) hash = 31 * hash + b;
        return hash;
    }

    private int hashCode2(byte[] bytes) {
        int hash = 0;
        for (byte b : bytes) hash = 17 * hash + b;
        return hash;
    }
}
