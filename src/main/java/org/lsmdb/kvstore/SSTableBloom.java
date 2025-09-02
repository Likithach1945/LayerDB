package org.lsmdb.kvstore;

import java.util.BitSet;

public class SSTableBloom {
    private final BitSet bitSet;
    private final int size;

    public SSTableBloom(int size) {
        this.size = size;
        this.bitSet = new BitSet(size);
    }

    public void add(String key) {
        int[] hashes = getHashes(key);
        for (int h : hashes) bitSet.set(Math.abs(h % size));
    }

    public boolean mightContain(String key) {
        int[] hashes = getHashes(key);
        for (int h : hashes) if (!bitSet.get(Math.abs(h % size))) return false;
        return true;
    }

    private int[] getHashes(String key) {
        int h1 = key.hashCode();
        int h2 = Integer.rotateLeft(h1, 16);
        return new int[]{h1, h2};
    }
}
