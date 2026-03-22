package az.zeynalov.tests;

import az.zeynalov.memtable.Arena;
import az.zeynalov.memtable.MemTable;
import az.zeynalov.memtable.MemTableIterator;
import az.zeynalov.memtable.SkipList;
import org.junit.jupiter.api.*;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive stress test for MemTable, MemTableIterator, and the underlying SkipList.
 * Exercises high-volume inserts/gets, MVCC semantics, concurrent read/write,
 * iterator traversal under mutation, large payloads, and hotspot races.
 */
@Tag("stress")
class MemTableStressTest {

    private Arena hotArena;
    private Arena coldArena;
    private SkipList skipList;
    private MemTable memTable;
    private java.lang.foreign.Arena testScope;

    // Cold-arena layout constants (must match SkipList)
    private static final int SN_LENGTH = 8;
    private static final int TYPE_LENGTH = 4;
    private static final int KEY_LENGTH = 4;
    private static final int VALUE_LENGTH = 4;
    private static final int KEY_LENGTH_OFFSET = SN_LENGTH + TYPE_LENGTH;

    @BeforeEach
    void setUp() {
        hotArena = new Arena();
        coldArena = new Arena();
        skipList = new SkipList(hotArena, coldArena);
        skipList.init();
        memTable = new MemTable(hotArena, coldArena, skipList);
        testScope = java.lang.foreign.Arena.ofShared();
    }

    @AfterEach
    void tearDown() {
        hotArena.close();
        if (testScope.scope().isAlive()) {
            testScope.close();
        }
    }

    // ─── Helpers ──────────────────────────────────────────────────────────────

    private MemorySegment seg(String s) {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        MemorySegment ms = testScope.allocate(bytes.length);
        ms.copyFrom(MemorySegment.ofArray(bytes));
        return ms;
    }

    private MemorySegment segBytes(byte[] bytes) {
        MemorySegment ms = testScope.allocate(bytes.length);
        ms.copyFrom(MemorySegment.ofArray(bytes));
        return ms;
    }

    /** Read key string from a hot-arena node offset. */
    private String readKey(int nodeOffset) {
        int coldOffset = hotArena.readInt(nodeOffset + 8 + 4); // PREFIX(8)+LEVEL_COUNT(4)
        int keyLen = coldArena.readInt(coldOffset + KEY_LENGTH_OFFSET);
        int keyOff = coldOffset + KEY_LENGTH_OFFSET + KEY_LENGTH + VALUE_LENGTH;
        MemorySegment keySeg = coldArena.readBytes(keyOff, keyLen);
        return new String(keySeg.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
    }

    /** Read value string from a hot-arena node offset. */
    private String readValue(int nodeOffset) {
        int coldOffset = hotArena.readInt(nodeOffset + 8 + 4);
        int keyLen = coldArena.readInt(coldOffset + KEY_LENGTH_OFFSET);
        int valLen = coldArena.readInt(coldOffset + KEY_LENGTH_OFFSET + KEY_LENGTH);
        int valOff = coldOffset + KEY_LENGTH_OFFSET + KEY_LENGTH + VALUE_LENGTH + keyLen;
        MemorySegment valSeg = coldArena.readBytes(valOff, valLen);
        return new String(valSeg.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
    }

    /** Read SN from a hot-arena node offset. */
    private long readSN(int nodeOffset) {
        int coldOffset = hotArena.readInt(nodeOffset + 8 + 4);
        return coldArena.readLong(coldOffset);
    }

    /** Read a big-endian int from a byte[] at the given offset. */
    private int readIntBE(byte[] data, int offset) {
        return ((data[offset] & 0xFF) << 24)
             | ((data[offset + 1] & 0xFF) << 16)
             | ((data[offset + 2] & 0xFF) << 8)
             |  (data[offset + 3] & 0xFF);
    }

    /** Extract key string from the byte[] returned by MemTable.get(). */
    private String extractKeyFromGetResult(byte[] result) {
        // Layout: [keySize(4)][valueSize(4)][key bytes][value bytes]
        int keySize = readIntBE(result, 0);
        int keyOff = KEY_LENGTH + VALUE_LENGTH;
        return new String(result, keyOff, keySize, StandardCharsets.UTF_8);
    }

    /** Extract value string from the byte[] returned by MemTable.get(). */
    private String extractValueFromGetResult(byte[] result) {
        int keySize = readIntBE(result, 0);
        int valSize = readIntBE(result, KEY_LENGTH);
        int valOff = KEY_LENGTH + VALUE_LENGTH + keySize;
        return new String(result, valOff, valSize, StandardCharsets.UTF_8);
    }

    private void runTasks(ExecutorService pool, List<Callable<Void>> tasks, int timeoutSeconds)
            throws Exception {
        List<Future<Void>> futures = pool.invokeAll(tasks);
        pool.shutdown();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        for (Future<Void> f : futures) {
            long left = deadline - System.nanoTime();
            if (left <= 0) throw new TimeoutException("Task timeout");
            f.get(left, TimeUnit.NANOSECONDS);
        }
        assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 1. HIGH-VOLUME SINGLE-THREADED PUT / GET
    // ═══════════════════════════════════════════════════════════════════════════

    @Test
    void highVolumeSingleThread_putAndGet_allKeysRetrievable() {
        int total = 50_000;

        MemorySegment[] keys = new MemorySegment[total];
        MemorySegment[] values = new MemorySegment[total];

        for (int i = 0; i < total; i++) {
            keys[i] = seg("key-" + String.format("%06d", i));
            values[i] = seg("val-" + String.format("%06d", i));
            memTable.put(keys[i], i + 1L, (byte) 1, values[i]);
        }

        // Verify every single key is retrievable
        for (int i = 0; i < total; i++) {
            MemTableIterator it = new MemTableIterator(skipList);
            it.seek(keys[i], i + 1L);
            assertTrue(it.isValid(), "Key must be found: key-" + String.format("%06d", i));

            byte[] result = memTable.get(it);
            assertNotNull(result, "MemTable.get must not return null for key-" + i);

            String foundKey = extractKeyFromGetResult(result);
            assertEquals("key-" + String.format("%06d", i), foundKey);

            String foundVal = extractValueFromGetResult(result);
            assertEquals("val-" + String.format("%06d", i), foundVal);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 2. ITERATOR: seekToFirst / next — full scan, sorted order
    // ═══════════════════════════════════════════════════════════════════════════

    @Test
    void iteratorFullScan_keysInSortedOrder() {
        String[] rawKeys = {"banana", "apple", "cherry", "date", "elderberry", "fig", "grape"};
        for (int i = 0; i < rawKeys.length; i++) {
            memTable.put(seg(rawKeys[i]), 1L, (byte) 1, seg("v-" + rawKeys[i]));
        }

        MemTableIterator it = new MemTableIterator(skipList);
        it.seekToFirst();

        List<String> scanned = new ArrayList<>();
        while (it.isValid()) {
            String key = readKey(it.getCurrent());
            scanned.add(key);
            it.next();
        }

        assertEquals(rawKeys.length, scanned.size(), "Iterator must visit all keys");

        // The skip list orders by descending key (highest first), so verify descending order
        List<String> sorted = new ArrayList<>(scanned);
        sorted.sort((a, b) -> {
            byte[] ab = a.getBytes(StandardCharsets.UTF_8);
            byte[] bb = b.getBytes(StandardCharsets.UTF_8);
            int len = Math.min(ab.length, bb.length);
            for (int i = 0; i < len; i++) {
                int cmp = Byte.compareUnsigned(bb[i], ab[i]); // descending
                if (cmp != 0) return cmp;
            }
            return Integer.compare(bb.length, ab.length); // descending
        });
        assertEquals(sorted, scanned, "Iterator must yield keys in descending order");
    }

    @Test
    void iteratorFullScan_highVolume_50kKeys() {
        int total = 50_000;
        for (int i = 0; i < total; i++) {
            memTable.put(seg("iter-" + String.format("%06d", i)), 1L, (byte) 1, seg("v" + i));
        }

        MemTableIterator it = new MemTableIterator(skipList);
        it.seekToFirst();

        int count = 0;
        String prev = null;
        while (it.isValid()) {
            String key = readKey(it.getCurrent());
            if (prev != null) {
                assertTrue(prev.compareTo(key) >= 0,
                        "Out-of-order (expected descending): '" + prev + "' < '" + key + "'");
            }
            prev = key;
            count++;
            it.next();
        }

        assertEquals(total, count, "Iterator must visit exactly " + total + " keys");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 3. MVCC — multiple versions of the same key, seek by SN
    // ═══════════════════════════════════════════════════════════════════════════

    @Test
    void mvcc_multipleVersions_seekReturnCorrectVersion() {
        String key = "user:42";
        // Insert 100 versions with SN 1..100
        for (int sn = 1; sn <= 100; sn++) {
            memTable.put(seg(key), sn, (byte) 1, seg("version-" + sn));
        }

        // Seek with SN = 50 should return the version with SN <= 50 (i.e. SN=50)
        MemTableIterator it50 = new MemTableIterator(skipList);
        it50.seek(seg(key), 50L);
        assertTrue(it50.isValid());
        long sn50 = readSN(it50.getCurrent());
        assertTrue(sn50 <= 50, "SN must be <= 50, got " + sn50);

        // Seek with SN = MAX should return the highest version (SN=100)
        MemTableIterator itMax = new MemTableIterator(skipList);
        itMax.seek(seg(key), Long.MAX_VALUE);
        assertTrue(itMax.isValid());
        long snMax = readSN(itMax.getCurrent());
        assertEquals(100L, snMax, "Seeking with MAX_VALUE SN should return highest version");

        // Seek with SN = 1 should return version 1
        MemTableIterator it1 = new MemTableIterator(skipList);
        it1.seek(seg(key), 1L);
        assertTrue(it1.isValid());
        long sn1 = readSN(it1.getCurrent());
        assertEquals(1L, sn1, "Seeking with SN=1 should return version 1");
    }

    @Test
    void mvcc_highVolumeVersions_correctness() {
        int versions = 5_000;
        String key = "mvcc-key";

        for (int sn = 1; sn <= versions; sn++) {
            memTable.put(seg(key), sn, (byte) 1, seg("v" + sn));
        }

        // Probe every 10th SN
        for (int targetSN = 1; targetSN <= versions; targetSN += 10) {
            MemTableIterator it = new MemTableIterator(skipList);
            it.seek(seg(key), targetSN);
            assertTrue(it.isValid(), "Must find version for SN=" + targetSN);
            long foundSN = readSN(it.getCurrent());
            assertTrue(foundSN <= targetSN,
                    "Found SN " + foundSN + " must be <= target " + targetSN);
            assertEquals(targetSN, foundSN,
                    "For exact SN insertion, should find exact match");
            String val = readValue(it.getCurrent());
            assertEquals("v" + targetSN, val);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 4. CONCURRENT WRITERS — unique keys, verify all present
    // ═══════════════════════════════════════════════════════════════════════════

    @Test
    void concurrentWriters_uniqueKeys_allPresent() throws Exception {
        int threads = Math.max(8, Runtime.getRuntime().availableProcessors() * 2);
        int keysPerThread = 3_000;
        int totalKeys = threads * keysPerThread;

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CyclicBarrier barrier = new CyclicBarrier(threads);

        List<Callable<Void>> tasks = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int tid = t;
            tasks.add(() -> {
                barrier.await();
                for (int i = 0; i < keysPerThread; i++) {
                    int id = tid * keysPerThread + i;
                    memTable.put(seg("ck-" + id), 1L, (byte) 1, seg("cv-" + id));
                }
                return null;
            });
        }

        runTasks(pool, tasks, 90);

        // Verify every key
        for (int i = 0; i < totalKeys; i++) {
            MemTableIterator it = new MemTableIterator(skipList);
            it.seek(seg("ck-" + i), 1L);
            assertTrue(it.isValid(), "Missing key after concurrent insert: ck-" + i);
            assertEquals("ck-" + i, readKey(it.getCurrent()));
        }

        // Also verify via full scan count
        MemTableIterator scan = new MemTableIterator(skipList);
        scan.seekToFirst();
        int count = 0;
        while (scan.isValid()) {
            count++;
            scan.next();
        }
        assertEquals(totalKeys, count, "Full scan count must match total inserted keys");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 5. CONCURRENT WRITERS — same key, many SNs (MVCC concurrency)
    // ═══════════════════════════════════════════════════════════════════════════

    @Test
    void concurrentWriters_sameKeyManySNs_noCorruption() throws Exception {
        int writers = Math.max(8, Runtime.getRuntime().availableProcessors() * 2);
        int versionsPerWriter = 500;
        String key = "hot-mvcc-key";

        ExecutorService pool = Executors.newFixedThreadPool(writers);
        CyclicBarrier barrier = new CyclicBarrier(writers);
        Set<String> validValues = ConcurrentHashMap.newKeySet();

        List<Callable<Void>> tasks = new ArrayList<>();
        for (int t = 0; t < writers; t++) {
            final int tid = t;
            tasks.add(() -> {
                barrier.await();
                for (int v = 0; v < versionsPerWriter; v++) {
                    long sn = ((long) tid << 20) | v;
                    String value = "w" + tid + "-v" + v;
                    validValues.add(value);
                    memTable.put(seg(key), sn, (byte) 1, seg(value));
                }
                return null;
            });
        }

        runTasks(pool, tasks, 60);

        // The latest SN version must be readable and its value must be valid
        MemTableIterator it = new MemTableIterator(skipList);
        it.seek(seg(key), Long.MAX_VALUE);
        assertTrue(it.isValid());
        String finalVal = readValue(it.getCurrent());
        assertTrue(validValues.contains(finalVal),
                "Final value must be from a writer, got: " + finalVal);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 6. MIXED CONCURRENT READ / WRITE — readers seek & iterate while writers mutate
    // ═══════════════════════════════════════════════════════════════════════════

    @Test
    void concurrentMixedReadWrite_noExceptionsOrCorruption() throws Exception {
        int writers = Math.max(4, Runtime.getRuntime().availableProcessors());
        int readers = Math.max(4, Runtime.getRuntime().availableProcessors());
        int keySpace = 5_000;
        int insertsPerWriter = 3_000;

        ExecutorService pool = Executors.newFixedThreadPool(writers + readers);
        CountDownLatch startGate = new CountDownLatch(1);
        AtomicBoolean stopReaders = new AtomicBoolean(false);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Set<String> validValues = ConcurrentHashMap.newKeySet();

        List<Callable<Void>> tasks = new ArrayList<>();

        // Writers
        for (int t = 0; t < writers; t++) {
            final int tid = t;
            tasks.add(() -> {
                startGate.await();
                ThreadLocalRandom rnd = ThreadLocalRandom.current();
                for (int i = 0; i < insertsPerWriter; i++) {
                    int k = rnd.nextInt(keySpace);
                    String key = "rw-" + k;
                    long sn = ((long) tid << 32) | i;
                    String value = "rw-v-" + tid + "-" + i;
                    validValues.add(value);
                    memTable.put(seg(key), sn, (byte) 1, seg(value));
                }
                return null;
            });
        }

        // Readers — mix of seek() and seekToFirst()/next()
        for (int r = 0; r < readers; r++) {
            tasks.add(() -> {
                startGate.await();
                ThreadLocalRandom rnd = ThreadLocalRandom.current();
                while (!stopReaders.get()) {
                    try {
                        if (rnd.nextBoolean()) {
                            // Random seek
                            String key = "rw-" + rnd.nextInt(keySpace);
                            MemTableIterator it = new MemTableIterator(skipList);
                            it.seek(seg(key), Long.MAX_VALUE);
                            if (it.isValid()) {
                                byte[] result = memTable.get(it);
                                assertNotNull(result);
                            }
                        } else {
                            // Partial scan
                            MemTableIterator it = new MemTableIterator(skipList);
                            it.seekToFirst();
                            int steps = rnd.nextInt(200);
                            while (it.isValid() && steps-- > 0) {
                                String val = readValue(it.getCurrent());
                                if (!validValues.contains(val)) {
                                    throw new AssertionError(
                                            "Reader saw invalid value: " + val);
                                }
                                it.next();
                            }
                        }
                    } catch (AssertionError e) {
                        failure.compareAndSet(null, e);
                        break;
                    }
                }
                return null;
            });
        }

        // Submit all, start, wait for writers, then stop readers
        List<Future<Void>> futures = new ArrayList<>();
        for (Callable<Void> task : tasks) {
            futures.add(pool.submit(() -> {
                try {
                    return task.call();
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                    throw t;
                }
            }));
        }
        startGate.countDown();

        long deadlineNs = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
        // Wait for writers
        for (int i = 0; i < writers; i++) {
            long left = deadlineNs - System.nanoTime();
            assertTrue(left > 0, "Timed out waiting for writer threads");
            futures.get(i).get(left, TimeUnit.NANOSECONDS);
        }

        stopReaders.set(true);

        // Wait for readers
        for (int i = writers; i < futures.size(); i++) {
            long left = deadlineNs - System.nanoTime();
            assertTrue(left > 0, "Timed out waiting for reader threads");
            futures.get(i).get(left, TimeUnit.NANOSECONDS);
        }

        pool.shutdownNow();
        assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        assertNull(failure.get(), "Failure during mixed read/write: " + failure.get());
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 7. CONCURRENT ITERATOR TRAVERSAL UNDER MUTATION
    // ═══════════════════════════════════════════════════════════════════════════

    @Test
    void concurrentIteratorTraversal_noExceptions() throws Exception {
        // Pre-fill 10k keys
        int prefill = 10_000;
        for (int i = 0; i < prefill; i++) {
            memTable.put(seg("pre-" + String.format("%06d", i)), 1L, (byte) 1, seg("pv" + i));
        }

        int writerThreads = 4;
        int iteratorThreads = 4;
        int additionalInserts = 5_000;

        ExecutorService pool = Executors.newFixedThreadPool(writerThreads + iteratorThreads);
        CyclicBarrier barrier = new CyclicBarrier(writerThreads + iteratorThreads);
        AtomicBoolean stopIterators = new AtomicBoolean(false);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicLong totalNodesScanned = new AtomicLong(0);

        List<Callable<Void>> tasks = new ArrayList<>();

        // Writers: insert more keys while iterators scan
        for (int t = 0; t < writerThreads; t++) {
            final int tid = t;
            tasks.add(() -> {
                barrier.await();
                for (int i = 0; i < additionalInserts; i++) {
                    int id = tid * additionalInserts + i;
                    memTable.put(seg("new-" + id), 1L, (byte) 1, seg("nv" + id));
                }
                return null;
            });
        }

        // Iterators: repeatedly scan from first to end
        for (int r = 0; r < iteratorThreads; r++) {
            tasks.add(() -> {
                barrier.await();
                while (!stopIterators.get()) {
                    try {
                        MemTableIterator it = new MemTableIterator(skipList);
                        it.seekToFirst();
                        int scanned = 0;
                        while (it.isValid()) {
                            readKey(it.getCurrent()); // ensure we can read without crash
                            it.next();
                            scanned++;
                        }
                        totalNodesScanned.addAndGet(scanned);
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                        break;
                    }
                }
                return null;
            });
        }

        List<Future<Void>> futures = new ArrayList<>();
        for (Callable<Void> task : tasks) {
            futures.add(pool.submit(task));
        }

        // Wait for writers
        long deadlineNs = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
        for (int i = 0; i < writerThreads; i++) {
            long left = deadlineNs - System.nanoTime();
            assertTrue(left > 0);
            futures.get(i).get(left, TimeUnit.NANOSECONDS);
        }

        stopIterators.set(true);

        for (int i = writerThreads; i < futures.size(); i++) {
            long left = deadlineNs - System.nanoTime();
            assertTrue(left > 0);
            futures.get(i).get(left, TimeUnit.NANOSECONDS);
        }

        pool.shutdownNow();
        assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        assertNull(failure.get(), "Iterator failure under concurrent mutation: " + failure.get());
        assertTrue(totalNodesScanned.get() > 0, "Iterators must have scanned some nodes");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 8. LARGE KEY AND VALUE PAYLOADS
    // ═══════════════════════════════════════════════════════════════════════════

    @Test
    void largePayloads_insertAndRetrieve() {
        int[] keySizes = {1, 7, 8, 15, 31, 32, 64, 128, 256, 512};
        int[] valueSizes = {1, 64, 256, 1024, 4096};

        int count = 0;
        for (int ks : keySizes) {
            for (int vs : valueSizes) {
                byte[] keyBytes = new byte[ks];
                byte[] valBytes = new byte[vs];
                // Deterministic fill
                for (int i = 0; i < ks; i++) keyBytes[i] = (byte) ((count * 31 + i) & 0xFF);
                for (int i = 0; i < vs; i++) valBytes[i] = (byte) ((count * 47 + i) & 0xFF);

                memTable.put(segBytes(keyBytes), count + 1L, (byte) 1, segBytes(valBytes));

                MemTableIterator it = new MemTableIterator(skipList);
                it.seek(segBytes(keyBytes), count + 1L);
                assertTrue(it.isValid(), "Key of size " + ks + " must be found");

                // Verify value from cold arena
                int coldOffset = hotArena.readInt(it.getCurrent() + 8 + 4);
                int foundKeyLen = coldArena.readInt(coldOffset + KEY_LENGTH_OFFSET);
                int foundValLen = coldArena.readInt(coldOffset + KEY_LENGTH_OFFSET + KEY_LENGTH);
                assertEquals(ks, foundKeyLen, "Key size mismatch");
                assertEquals(vs, foundValLen, "Value size mismatch");

                int valOff = coldOffset + KEY_LENGTH_OFFSET + KEY_LENGTH + VALUE_LENGTH + foundKeyLen;
                MemorySegment valSeg = coldArena.readBytes(valOff, foundValLen);
                assertArrayEquals(valBytes, valSeg.toArray(ValueLayout.JAVA_BYTE),
                        "Value bytes mismatch for keySize=" + ks + " valSize=" + vs);

                count++;
            }
        }
        assertTrue(count > 0, "Must have tested at least some combos");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 9. REPEATED HOTSPOT RACE — small key space, high contention
    // ═══════════════════════════════════════════════════════════════════════════

    @RepeatedTest(10)
    void hotspotRace_tinyKeySpace_noCorruptReads() throws Exception {
        int threads = Math.max(8, Runtime.getRuntime().availableProcessors() * 2);
        int opsPerThread = 5_000;
        int hotKeys = 8;

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CyclicBarrier barrier = new CyclicBarrier(threads);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Set<String> validValues = ConcurrentHashMap.newKeySet();

        List<Callable<Void>> tasks = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int tid = t;
            tasks.add(() -> {
                barrier.await();
                ThreadLocalRandom rnd = ThreadLocalRandom.current();
                for (int i = 0; i < opsPerThread; i++) {
                    String key = "hk-" + rnd.nextInt(hotKeys);
                    if (rnd.nextInt(100) < 60) {
                        // Write
                        long sn = ((long) tid << 32) | i;
                        String value = "hv-" + tid + "-" + i;
                        validValues.add(value);
                        memTable.put(seg(key), sn, (byte) 1, seg(value));
                    } else {
                        // Read via seek
                        MemTableIterator it = new MemTableIterator(skipList);
                        it.seek(seg(key), Long.MAX_VALUE);
                        if (it.isValid()) {
                            String observed = readValue(it.getCurrent());
                            if (!validValues.contains(observed)) {
                                throw new AssertionError(
                                        "Invalid value under hotspot race: " + observed);
                            }
                        }
                    }
                }
                return null;
            });
        }

        try {
            runTasks(pool, tasks, 60);
        } catch (Throwable t) {
            failure.compareAndSet(null, t);
        }

        assertNull(failure.get(), "Hotspot race failure: " + failure.get());
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 10. MEMTABLE.GET() VIA ITERATOR — bulk validation
    // ═══════════════════════════════════════════════════════════════════════════

    @Test
    void memTableGet_viaIterator_bulkValidation() {
        int total = 20_000;

        for (int i = 0; i < total; i++) {
            String k = "mg-" + String.format("%06d", i);
            String v = "mv-" + String.format("%06d", i);
            memTable.put(seg(k), 1L, (byte) 1, seg(v));
        }

        // Point-get every key
        for (int i = 0; i < total; i++) {
            String k = "mg-" + String.format("%06d", i);
            MemTableIterator it = new MemTableIterator(skipList);
            it.seek(seg(k), 1L);
            assertTrue(it.isValid(), "Key must exist: " + k);

            byte[] result = memTable.get(it);
            assertNotNull(result, "MemTable.get() must return non-null for " + k);

            String extractedKey = extractKeyFromGetResult(result);
            String extractedVal = extractValueFromGetResult(result);
            assertEquals(k, extractedKey);
            assertEquals("mv-" + String.format("%06d", i), extractedVal);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 11. SEEK MISS — keys that were never inserted must return invalid
    // ═══════════════════════════════════════════════════════════════════════════

    @Test
    void seekMiss_returnsInvalidIterator() {
        // Insert some keys
        for (int i = 0; i < 1_000; i++) {
            memTable.put(seg("exists-" + i), 1L, (byte) 1, seg("v" + i));
        }

        // Seek keys that don't exist
        for (int i = 0; i < 5_000; i++) {
            MemTableIterator it = new MemTableIterator(skipList);
            it.seek(seg("missing-" + i), 1L);
            assertFalse(it.isValid(), "Missing key must yield invalid iterator: missing-" + i);

            byte[] result = memTable.get(it);
            assertNull(result, "MemTable.get() must return null for missing key");
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 12. CONCURRENT SEEK + MEMTABLE.GET — full end-to-end under contention
    // ═══════════════════════════════════════════════════════════════════════════

    @Test
    void concurrentSeekAndGet_endToEnd() throws Exception {
        // Pre-fill
        int prefill = 10_000;
        for (int i = 0; i < prefill; i++) {
            memTable.put(seg("e2e-" + String.format("%06d", i)), 1L, (byte) 1,
                    seg("e2e-v-" + String.format("%06d", i)));
        }

        int threads = Math.max(8, Runtime.getRuntime().availableProcessors() * 2);
        int readsPerThread = 10_000;

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CyclicBarrier barrier = new CyclicBarrier(threads);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        List<Callable<Void>> tasks = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            tasks.add(() -> {
                barrier.await();
                ThreadLocalRandom rnd = ThreadLocalRandom.current();
                for (int i = 0; i < readsPerThread; i++) {
                    int idx = rnd.nextInt(prefill);
                    String key = "e2e-" + String.format("%06d", idx);
                    MemTableIterator it = new MemTableIterator(skipList);
                    it.seek(seg(key), 1L);
                    if (!it.isValid()) {
                        throw new AssertionError("Key not found during concurrent seek: " + key);
                    }
                    byte[] result = memTable.get(it);
                    if (result == null) {
                        throw new AssertionError("Null result from MemTable.get for " + key);
                    }
                    String foundKey = extractKeyFromGetResult(result);
                    if (!key.equals(foundKey)) {
                        throw new AssertionError(
                                "Key mismatch: expected=" + key + " got=" + foundKey);
                    }
                }
                return null;
            });
        }

        try {
            runTasks(pool, tasks, 90);
        } catch (Throwable t) {
            failure.compareAndSet(null, t);
        }

        assertNull(failure.get(), "Concurrent seek+get failed: " + failure.get());
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 13. TOMBSTONE TYPE — insert with different type bytes
    // ═══════════════════════════════════════════════════════════════════════════

    @Test
    void tombstoneType_insertAndRead() {
        // Insert a value, then a tombstone with higher SN
        memTable.put(seg("tomb-key"), 1L, (byte) 1, seg("alive-value"));
        memTable.put(seg("tomb-key"), 2L, (byte) 0, seg("")); // tombstone

        // Latest version (SN=MAX) should see the tombstone
        MemTableIterator it = new MemTableIterator(skipList);
        it.seek(seg("tomb-key"), Long.MAX_VALUE);
        assertTrue(it.isValid());
        long sn = readSN(it.getCurrent());
        assertEquals(2L, sn, "Should see the tombstone version");

        // SN=1 should see the original value
        MemTableIterator it1 = new MemTableIterator(skipList);
        it1.seek(seg("tomb-key"), 1L);
        assertTrue(it1.isValid());
        assertEquals(1L, readSN(it1.getCurrent()));
        assertEquals("alive-value", readValue(it1.getCurrent()));
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // 14. EDGE CASES — empty memtable, single entry, boundary keys
    // ═══════════════════════════════════════════════════════════════════════════

    @Test
    void emptyMemTable_seekReturnsInvalid() {
        MemTableIterator it = new MemTableIterator(skipList);
        it.seek(seg("anything"), 1L);
        assertFalse(it.isValid());
        assertNull(memTable.get(it));
    }

    @Test
    void emptyMemTable_seekToFirstReturnsInvalid() {
        MemTableIterator it = new MemTableIterator(skipList);
        it.seekToFirst();
        assertFalse(it.isValid());
    }

    @Test
    void singleEntry_seekAndIterate() {
        memTable.put(seg("only-key"), 42L, (byte) 1, seg("only-value"));

        MemTableIterator it = new MemTableIterator(skipList);
        it.seek(seg("only-key"), 42L);
        assertTrue(it.isValid());
        assertEquals("only-key", readKey(it.getCurrent()));
        assertEquals("only-value", readValue(it.getCurrent()));

        it.next();
        assertFalse(it.isValid(), "After single entry, next must be invalid");
    }

    @Test
    void singleByteKey_andSingleByteValue() {
        memTable.put(segBytes(new byte[]{0x01}), 1L, (byte) 1, segBytes(new byte[]{(byte) 0xFF}));

        MemTableIterator it = new MemTableIterator(skipList);
        it.seek(segBytes(new byte[]{0x01}), 1L);
        assertTrue(it.isValid());

        int coldOffset = hotArena.readInt(it.getCurrent() + 8 + 4);
        int keyLen = coldArena.readInt(coldOffset + KEY_LENGTH_OFFSET);
        int valLen = coldArena.readInt(coldOffset + KEY_LENGTH_OFFSET + KEY_LENGTH);
        assertEquals(1, keyLen);
        assertEquals(1, valLen);
    }
}

