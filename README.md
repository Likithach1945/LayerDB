# LayerDB

LayerDB is a **file-based LSM (Log-Structured Merge) Tree key-value datastore** that supports **Create, Read, and Delete (CRD) operations**. It is designed for **local storage in a single process** with high-performance memory and disk management.

Inspired by Freshworks' naming conventions, LayerDB is implemented with **MemTables, SSTables, WAL, and compaction strategies** to provide reliable and efficient storage.

---

## Features

### Functional
- Initialize the database with a **custom data directory**; defaults are created if none provided.
- **Insert or update** key-value pairs (`put(key, value)`), where both keys and values are strings.
- **Delete keys** using `delete(key)`, with tombstone handling for LSM consistency.
- **Retrieve values** using `get(key)`. Returns `null` if the key does not exist or has expired.
- **Write-Ahead Logging (WAL)** ensures durability of writes.
- **MemTable flushing:** Data is written to disk as **SSTables** when thresholds are reached.
- **Compaction strategies:**
    - **Size-based compaction** to avoid exceeding disk limits.
    - **File-count-based compaction** to maintain manageable SSTable numbers.
- **Bloom filters** per SSTable for fast key existence checks.
- Supports **compressed** and **uncompressed** SSTables (`.gz`).

### Non-Functional
- **Thread-safe**: multiple threads can safely access the same instance.
- Efficient **memory usage** with background flushing.
- Maintains **high performance** with minimal latency.
- **Disk-friendly**: handles large datasets using SSTables and compaction.

---

## Architecture Overview

| Component      | Description |
|----------------|-------------|
| **MemTable**   | In-memory sorted key-value store. Triggers flush when size exceeds threshold. |
| **WAL**        | Logs every write before applying to MemTable to ensure durability. |
| **SSTable**    | Immutable disk storage of flushed MemTables. Supports optional compression. |
| **Compaction** | Merges SSTables to remove tombstones and reduce file count or disk size. |
| **BloomFilter**| Helps quickly check key existence in SSTables to minimize disk reads. |

---

## Performance Benchmarks

LayerDB has been benchmarked against **SQLite (B-tree based)** to compare insertion throughput and read latencies.

---

### Write Performance

| Test Case     | LayerDB Time (ms) | SQLite Time (ms) | Speedup        |
|---------------|------------------:|-----------------:|----------------|
| insert100     | 14.7              | 60.3             | ~4.1× faster   |
| insert1,000   | 25.3              | 311.4            | ~12.3× faster  |
| insert10,000  | 59.0              | 46,157           | ~781× faster   |

---

### Read Performance

| Test Case     | LayerDB Time (ms) | SQLite Time (ms) | Relative        |
|---------------|------------------:|-----------------:|-----------------|
| read100       | 2.9               | 0.15             | ~19× slower     |
| read1,000     | 12.5              | 0.91             | ~13× slower     |
| read10,000    | 180.2             | 9.4              | ~19× slower     |

---

### Observation
LayerDB demonstrates **orders-of-magnitude faster writes** compared to SQLite due to its **LSM-based design**,  
but has **slower reads** because of SSTable lookups — a classic trade-off in LSM tree storage engines.

---
## 📊 Benchmark Results

### Insert Operations

**insertHundredKeys() took:**
- 14.7256 milliseconds
- 0.0147 seconds

**insertThousandKeys() took:**
- 25.365 milliseconds
- 0.0253 seconds

**insertTenThousandKeys() took:**
- 59.0079 milliseconds
- 0.0590 seconds

**insertTwentyThousandKeys() took:**
- 167.5804 milliseconds
- 0.1676 seconds

---
## Usage Example

```java
LSMDatabase db = new LSMDatabase("data/");
db.put("k1", "v1");
db.put("k2", "v2");

String value = db.get("k1");   // returns "v1"
db.delete("k2");               // deletes key "k2"

db.flush();                     // forces MemTable flush to SSTable
