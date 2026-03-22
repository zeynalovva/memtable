# LSM MemTable

A high-performance, lock-free **MemTable** implementation for LSM-tree based storage engines, written in Java 23 using the [Foreign Memory API](https://docs.oracle.com/en/java/javase/23/core/foreign-function-and-memory-api.html) (`java.lang.foreign`). All data lives **off-heap** in custom arena allocators, and the index structure is a concurrent skip list that supports **MVCC** (Multi-Version Concurrency Control) out of the box.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
  - [Arena – Off-Heap Memory Allocator](#arena--off-heap-memory-allocator)
  - [SkipList – Lock-Free Concurrent Index](#skiplist--lock-free-concurrent-index)
  - [MemTable – Public API Façade](#memtable--public-api-façade)
  - [MemTableIterator – Ordered Traversal](#memtableiterator--ordered-traversal)
- [Memory Layout](#memory-layout)
  - [Hot Arena (Index Nodes)](#hot-arena-index-nodes)
  - [Cold Arena (Key-Value Data)](#cold-arena-key-value-data)
- [MVCC Semantics](#mvcc-semantics)
- [Concurrency Model](#concurrency-model)
  - [Lock-Free Arena Allocation](#lock-free-arena-allocation)
  - [Lock-Free Skip List Insertion](#lock-free-skip-list-insertion)
  - [Atomic Level Advancement](#atomic-level-advancement)
  - [Acquire / Release Pointer Reads and Writes](#acquire--release-pointer-reads-and-writes)
  - [Thread-Local Update Cache](#thread-local-update-cache)
- [Key Comparison Strategy](#key-comparison-strategy)
- [How to Use](#how-to-use)
  - [Prerequisites](#prerequisites)
  - [Building](#building)
  - [Basic Usage](#basic-usage)
  - [Point Lookups with MVCC](#point-lookups-with-mvcc)
  - [Range Scans with the Iterator](#range-scans-with-the-iterator)
  - [Cleanup](#cleanup)
- [Running Tests](#running-tests)
- [Benchmarks](#benchmarks)
- [Project Structure](#project-structure)
- [Design Decisions & Trade-offs](#design-decisions--trade-offs)
- [License](#license)

---

## Overview

In a Log-Structured Merge-tree (LSM-tree) storage engine, the **MemTable** is the in-memory write buffer that absorbs all incoming writes before they are flushed to on-disk sorted runs (SSTables). This project implements a MemTable with the following goals:

| Goal | How it's achieved |
|---|---|
| **High write throughput** | Lock-free skip list with CAS-based insertion |
| **Off-heap memory** | Java Foreign Memory API arenas – zero GC pressure for data |
| **MVCC support** | Every key is tagged with a sequence number (SN); multiple versions coexist |
| **Ordered iteration** | Skip list maintains sorted order; iterator supports `seekToFirst`, `seek`, and `next` |
| **Predictable memory budget** | Each arena is a fixed 64 MB slab; an `ArenaCapacityException` is thrown when full |

---

## Architecture

The implementation is composed of four main components:

```
┌──────────────────────────────────────────────────────┐
│                      MemTable                        │
│         (public API: put, get via iterator)           │
└──────────────┬──────────────────────┬────────────────┘
               │                      │
       ┌───────▼───────┐      ┌───────▼────────┐
       │   SkipList     │      │ MemTableIterator│
       │  (lock-free    │      │ (seek, next,    │
       │   concurrent   │      │  seekToFirst)   │
       │   index)       │      └────────────────┘
       └───┬────────┬───┘
           │        │
    ┌──────▼──┐  ┌──▼───────┐
    │Hot Arena│  │Cold Arena │
    │ (index  │  │ (key/val  │
    │  nodes) │  │  payloads)│
    └─────────┘  └──────────┘
```

### Arena – Off-Heap Memory Allocator

**`Arena.java`** is a bump-pointer allocator backed by a single 64 MB `MemorySegment` allocated via `java.lang.foreign.Arena.ofShared()`. It provides:

- **`allocate(int size)`** – Thread-safe bump allocation via `AtomicInteger` CAS. Returns the aligned offset (8-byte aligned) within the arena.
- **Read/write primitives** – `readInt`, `readLong`, `readByte`, `readBytes`, `writeInt`, `writeLong`, `writeByte`, `writeBytes` – all using big-endian byte order for portable, deterministic lexicographic comparison.
- **`close()`** – Releases the underlying off-heap memory.

The arena never frees individual allocations; it is designed to be used for the lifetime of a single MemTable and then discarded wholesale (the classic "arena allocation" pattern).

### SkipList – Lock-Free Concurrent Index

**`SkipList.java`** is a concurrent, lock-free skip list where both the index structure (forward pointers, prefix cache) and the key-value payloads are stored in arena memory. It is the core data structure behind the MemTable.

Key properties:
- **Max level:** 12
- **Promotion probability:** 0.25 (on average ≈ 1.33 pointers per node)
- **Ordering:** Keys are compared as unsigned byte sequences (lexicographic). For equal keys, a higher sequence number (SN) sorts *first* (newer versions come first), enabling efficient MVCC lookups.

### MemTable – Public API Façade

**`MemTable.java`** is a thin wrapper that owns a hot arena, a cold arena, and a skip list. It exposes:

- **`put(key, SN, type, value)`** – Insert a versioned key-value pair.
- **`get(iterator)`** – Read the key size, value size, key bytes, and value bytes at the iterator's current position.

### MemTableIterator – Ordered Traversal

**`MemTableIterator.java`** provides positioned, forward-only iteration over the skip list:

| Method | Description |
|---|---|
| `seekToFirst()` | Position at the very first (smallest) key in the skip list |
| `seek(key)` | Position at the given key (latest version, i.e., `SN = Long.MAX_VALUE`) |
| `seek(key, SN)` | Position at the given key at or before the specified sequence number |
| `next()` | Advance to the next node in sorted order |
| `isValid()` | Returns `true` if the iterator points to a valid node |
| `getCurrent()` | Returns the raw hot-arena offset of the current node |

---

## Memory Layout

Data is split across two arenas to keep the hot-path cache-friendly.

### Hot Arena (Index Nodes)

Each skip list node in the hot arena has the following layout:

```
┌─────────────────┬──────────────┬──────────────────┬──────────────┬───┬──────────────┐
│  Prefix (8 B)   │ Level Count  │ Cold Arena Offset│  Next Ptr[0] │...│ Next Ptr[N-1]│
│                 │   (4 B)      │     (4 B)        │   (4 B)      │   │   (4 B)      │
└─────────────────┴──────────────┴──────────────────┴──────────────┴───┴──────────────┘
```

- **Prefix (8 bytes):** The first 8 bytes of the key, cached inline for fast comparison without touching the cold arena.
- **Level Count (4 bytes):** Number of skip list levels this node participates in.
- **Cold Arena Offset (4 bytes):** Pointer into the cold arena where the full key-value record lives.
- **Next Pointers (4 bytes each):** One per level; each stores the hot-arena offset of the next node at that level, or `-1` for null.

### Cold Arena (Key-Value Data)

Each key-value record in the cold arena:

```
┌──────────┬──────────┬──────────┬────────────┬──────────┬────────────┐
│  SN (8B) │ Type(4B) │ Key Size │ Value Size │ Key Bytes│ Value Bytes│
│          │          │  (4 B)   │   (4 B)    │ (var)    │   (var)    │
└──────────┴──────────┴──────────┴────────────┴──────────┴────────────┘
```

- **SN (8 bytes):** The MVCC sequence number.
- **Type (4 bytes):** Operation type (e.g., `0` for put, could be used for tombstone markers in a delete scenario).
- **Key Size / Value Size (4 bytes each):** Lengths of the variable-size key and value.
- **Key Bytes / Value Bytes:** The raw payload.

---

## MVCC Semantics

Every key-value pair is tagged with a **sequence number (SN)**. The skip list maintains multiple versions of the same key, ordered by SN descending (newest first). This enables snapshot reads:

```
Key: "user:42"
  ├── (SN=130, type=tombstone, value="")      ← newest
  ├── (SN=122, type=put,      value="Al")
  └── (SN=120, type=put,      value="Alicia") ← oldest
```

A point lookup for `("user:42", SN=125)` returns the record with **SN=122** – the latest version whose sequence number is **≤ 125**. This is the standard MVCC read semantic used by databases like LevelDB, RocksDB, and Pebble.

The `compare` method in `SkipList` implements this by first comparing keys lexicographically and, when keys are equal, comparing sequence numbers so that *higher* SNs sort first. The `get(key, SN)` method traverses the list and finds the first node whose key matches and whose SN ≤ the requested SN.

---

## Concurrency Model

The MemTable is designed for **concurrent reads and writes without locks**. Here is how each piece of the concurrency puzzle fits together:

### Lock-Free Arena Allocation

`Arena.allocate()` uses an `AtomicInteger` CAS loop to bump the offset pointer:

```java
int current, alignedOffset, next;
do {
    current = availableOffset.get();
    alignedOffset = (current + 7) & ~7;   // 8-byte alignment
    next = alignedOffset + sizeOfPayload;
    if (next > ALLOCATED_MEMORY_SIZE) {
        throw ArenaCapacityException.of(ErrorMessage.ARENA_IS_FULL);
    }
} while (!availableOffset.compareAndSet(current, next));
```

Multiple threads can allocate simultaneously; each thread atomically reserves a contiguous region. Because the arena is append-only (no deallocation), this is safe and wait-free in the common case (the CAS succeeds on the first try under low contention).

### Lock-Free Skip List Insertion

Insertion follows the classic lock-free skip list algorithm adapted for arena-based storage:

1. **Find position:** Traverse from the highest level down to level 0, recording the predecessor at each level in a thread-local `update[]` array.
2. **Allocate node:** Allocate space in both the cold arena (for the key-value record) and the hot arena (for the index node). This is safe because arena allocation is atomic.
3. **Link bottom-up with CAS:** For each level (0 → `newLevel`):
   - Set the new node's forward pointer to the expected next node.
   - CAS the predecessor's forward pointer from the expected value to the new node.
   - If the CAS fails (another thread inserted between the predecessor and the expected next), **re-traverse** from the predecessor at that level to find the correct insertion point, then retry.

```java
for (int i = 0; i <= newLevel; i++) {
    while (true) {
        int expected = readNext(i, update[i]);
        // If another node was inserted ahead of us, re-scan to find the correct predecessor
        if (!isNull(expected) && compare(expected, targetPrefix, SN, key) > 0) {
            int cur = update[i];
            while (true) {
                int next = readNext(i, cur);
                if (isNull(next) || compare(next, targetPrefix, SN, key) <= 0) break;
                cur = next;
            }
            update[i] = cur;
            continue;
        }
        writeNext(newNode, i, expected);
        if (casNext(update[i], i, expected, newNode)) break;
    }
}
```

This guarantees linearizable inserts without any mutex or `synchronized` block.

### Atomic Level Advancement

The skip list's current maximum level is stored in a plain `int` field, but accessed through a `VarHandle` for atomic compare-and-set:

```java
private static final VarHandle LEVEL_HANDLE;
// ...
while ((witness = (int) LEVEL_HANDLE.get(this)) < newLevel) {
    if ((boolean) LEVEL_HANDLE.compareAndSet(this, witness, newLevel)) break;
}
```

This ensures that when a node promotes the skip list to a new level, the update is visible to all threads.

### Acquire / Release Pointer Reads and Writes

Forward pointers are read with **acquire** semantics and written with **release** semantics using a `VarHandle` over the `MemorySegment`:

```java
// Read: getAcquire ensures we see all writes that happened before the pointer was set
private int readNext(int index, int offset) {
    int nextNodeOffset = offset + HOT_PATH_METADATA + (POINTER_SIZE * index);
    return (int) UPDATE_CACHE_HANDLE.getAcquire(hotArena.getMemory(), (long) nextNodeOffset);
}

// Write: setRelease ensures all preceding writes (node data) are visible before the pointer
private void writeNext(int nodeOffset, int level, int value) {
    int nextNodeOffset = nodeOffset + HOT_PATH_METADATA + (POINTER_SIZE * level);
    UPDATE_CACHE_HANDLE.setRelease(hotArena.getMemory(), (long) nextNodeOffset, value);
}
```

This acquire/release pairing is critical: it guarantees that when thread B follows a pointer that thread A wrote, thread B will see the fully constructed node (all key-value data in both arenas) that thread A wrote before publishing the pointer. This is the same memory-ordering pattern used in `java.util.concurrent.ConcurrentSkipListMap`.

### Thread-Local Update Cache

The `update[]` array (which records predecessors at each level during insertion) is kept in a `ThreadLocal<int[]>`. This avoids allocating a new array on every insert call and eliminates contention on a shared scratch buffer.

```java
private final ThreadLocal<int[]> updateCache = ThreadLocal.withInitial(
    () -> new int[MAX_LEVEL + 1]);
```

---

## Key Comparison Strategy

Comparisons are optimized with a **prefix-cache** technique:

1. **Prefix comparison (fast path):** The first 8 bytes of each key are stored inline in the hot arena node. These 8 bytes are compared as unsigned longs (`Long.compareUnsigned`). If they differ, the result is returned immediately – the cold arena is never touched.

2. **Full key comparison (slow path):** If prefixes match, the full key bytes are read from the cold arena and compared:
   - For short keys (< 32 bytes): a byte-by-byte loop with `Byte.compareUnsigned`.
   - For longer keys: `MemorySegment.mismatch()`, which uses SIMD-optimized vectorized comparison on supported platforms.

3. **Sequence number tiebreaker:** If keys are identical, the SN is compared so that **higher (newer) SNs sort first**.

---

## How to Use

### Prerequisites

- **Java 23+** (uses the Foreign Memory API which is stable in Java 22+)
- **Maven 3.8+**

### Building

```bash
git clone https://github.com/zeynalovvabbas/lsm-memtable.git
cd lsm-memtable
mvn clean compile
```

### Basic Usage

```java
import az.zeynalov.memtable.*;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;

// 1. Create arenas and the skip list
Arena hotArena  = new Arena();   // 64 MB off-heap for index nodes
Arena coldArena = new Arena();   // 64 MB off-heap for key-value data
SkipList skipList = new SkipList(hotArena, coldArena);
skipList.init();

// 2. Create the MemTable
MemTable memTable = new MemTable(hotArena, coldArena, skipList);

// 3. Insert key-value pairs with sequence numbers
MemorySegment key   = MemorySegment.ofArray("user:42".getBytes(StandardCharsets.UTF_8));
MemorySegment value = MemorySegment.ofArray("Alicia".getBytes(StandardCharsets.UTF_8));

long sequenceNumber = 100L;
byte operationType  = 0;  // 0 = put

memTable.put(key, sequenceNumber, operationType, value);
```

### Point Lookups with MVCC

```java
// Create an iterator for reads
MemTableIterator iterator = new MemTableIterator(skipList);

// Seek to a specific key at a specific snapshot (sequence number)
MemorySegment lookupKey = MemorySegment.ofArray("user:42".getBytes(StandardCharsets.UTF_8));
iterator.seek(lookupKey, 150L);  // find latest version with SN ≤ 150

if (iterator.isValid()) {
    byte[] result = memTable.get(iterator);
    // result layout: [keySize (4B)][valueSize (4B)][keyBytes][valueBytes]
    // Parse as needed...
}
```

### Range Scans with the Iterator

```java
MemTableIterator iterator = new MemTableIterator(skipList);

// Scan all entries from the beginning
iterator.seekToFirst();

while (iterator.isValid()) {
    byte[] record = memTable.get(iterator);
    // Process record...
    
    iterator.next();
}
```

### Cleanup

Both arenas implement `AutoCloseable`. When the MemTable is no longer needed (e.g., after flushing to an SSTable), close the arenas to release off-heap memory:

```java
hotArena.close();
coldArena.close();
```

Or use try-with-resources:

```java
try (Arena hotArena = new Arena(); Arena coldArena = new Arena()) {
    SkipList skipList = new SkipList(hotArena, coldArena);
    skipList.init();
    MemTable memTable = new MemTable(hotArena, coldArena, skipList);
    
    // ... use memTable ...
}  // arenas are automatically closed
```

---

## Running Tests

```bash
# Run all tests
mvn test

# Run stress tests specifically
mvn test -Dgroups=stress
```

The test suite includes:

| Test Class | Description |
|---|---|
| `ArenaTest` | Unit tests for arena allocation, read/write primitives, lifecycle |
| `SkipListTest` | Insertion, retrieval, ordering, MVCC version resolution |
| `SkipListBoundsStressTest` | Boundary conditions and edge cases under load |
| `SkipListRigorousConcurrencyTest` | Multi-threaded concurrent read/write correctness |
| `MemTableStressTest` | End-to-end stress test with concurrent puts, gets, iterator scans, MVCC, large payloads, and hotspot races |

---

## Benchmarks

Benchmarks are run using [JMH](https://openjdk.org/projects/code-tools/jmh/) and compare the arena-based skip list against `ConcurrentSkipListMap` and `TreeMap`.

```bash
mvn clean package
java -jar target/benchmarks.jar
```

### Selected Results (Optimized, Latest Run – Throughput, ops/μs, higher is better)

| Benchmark | 1K keys | 10K keys | 100K keys |
|---|---|---|---|
| **Get Hit (Arena)** | 5.32 | 3.32 | 1.92 |
| **Get Hit (CSLM)** | 17.81 | 6.93 | 3.83 |
| **Get Miss (Arena)** | 46.74 | 33.21 | 30.35 |
| **Get Miss (CSLM)** | 28.39 | 24.47 | 15.76 |
| **Insert (Arena)** | 6.94 | 5.14 | 4.38 |
| **Insert (CSLM)** | — | — | — |
| **Scan (Arena)** | 0.51 | 0.05 | 0.005 |
| **Scan (CSLM)** | 0.71 | 0.06 | 0.005 |

> **Note:** The arena-based skip list pays a higher per-lookup cost (due to off-heap indirection) but achieves significantly faster **miss** lookups thanks to the prefix cache that short-circuits comparisons. Scan performance is comparable. The real win is the memory model: all data lives off-heap with zero GC pressure, making this suitable for multi-GB MemTables where GC pauses would otherwise be a problem.

---

## Project Structure

```
src/
├── main/java/az/zeynalov/memtable/
│   ├── Arena.java                  # Off-heap bump-pointer allocator
│   ├── MemTable.java               # Public API façade
│   ├── MemTableIterator.java       # Positioned forward iterator
│   ├── SkipList.java               # Lock-free concurrent skip list
│   ├── benchmark/
│   │   └── SkipListBenchmark.java  # JMH benchmarks
│   └── exception/
│       ├── ArenaCapacityException.java
│       └── ErrorMessage.java
└── test/java/az/zeynalov/tests/
    ├── ArenaTest.java
    ├── MemTableStressTest.java
    ├── SkipListBoundsStressTest.java
    ├── SkipListRigorousConcurrencyTest.java
    └── SkipListTest.java
```

---

## Design Decisions & Trade-offs

| Decision | Rationale |
|---|---|
| **Two separate arenas (hot / cold)** | Index traversal only touches the hot arena. Key-value payloads are in the cold arena and are only read on a confirmed hit. This improves CPU cache utilization during lookups. |
| **8-byte prefix cache** | Most keys share a common prefix (e.g., `user:`) but diverge within the first 8 bytes. The prefix is compared as a single `long`, avoiding cold-arena reads for the vast majority of comparisons. |
| **Big-endian byte order** | Ensures that unsigned long comparison of the prefix produces the same result as lexicographic comparison of the key bytes. This is essential for the prefix optimization to be correct. |
| **8-byte alignment in arena** | All allocations are 8-byte aligned (`(current + 7) & ~7`). This allows the JVM and CPU to perform aligned reads of `long` and `int` values without penalties, and is required for `VarHandle` atomics on `MemorySegment`. |
| **`-1` as null sentinel** | Arena offsets are always non-negative, so `-1` is used to represent null pointers (no next node). This avoids the need for a separate null-check structure. |
| **Thread-local update cache** | The `update[]` array is reused across inserts within the same thread, avoiding per-insert allocation and reducing GC pressure. |
| **`VarHandle` instead of `Unsafe`** | The `VarHandle` API is the supported, future-proof way to do atomic operations in modern Java. It works directly on `MemorySegment` data, unlike `Unsafe` which is being phased out. |
| **No deletion from the skip list** | In LSM-tree architectures, deletes are represented as tombstone records (a special `type` byte). The skip list itself never removes nodes. Memory is reclaimed by closing the entire arena after flushing to disk. |
| **Fixed 64 MB arena size** | Provides a predictable memory budget. When the arena is full, `ArenaCapacityException` is thrown, signaling the LSM engine to flush the MemTable to an SSTable and create a new one. |
| **`Arena.ofShared()` scope** | The shared scope allows the `MemorySegment` to be accessed from multiple threads, which is required for the concurrent skip list. |

---

## License

This project is available under the [MIT License](LICENSE).

