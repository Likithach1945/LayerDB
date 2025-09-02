package org.lsmdb.kvstore;

/**
 * Shared constants for LSM DB (tombstone marker, etc.)
 */
public final class KVConstants {
    private KVConstants() {}

    /** canonical tombstone marker stored in WAL and SSTables */
    public static final String TOMBSTONE = "__TOMBSTONE__";
}
