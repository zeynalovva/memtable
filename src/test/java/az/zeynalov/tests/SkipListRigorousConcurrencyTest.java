package az.zeynalov.tests;

import az.zeynalov.memtable.Arena;
import az.zeynalov.memtable.SkipList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@Tag("concurrency")
class SkipListRigorousConcurrencyTest {

  private Arena hotArena;
  private Arena coldArena;
  private SkipList skipList;
  private java.lang.foreign.Arena testScope;

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
    testScope = java.lang.foreign.Arena.ofShared();
  }

  @AfterEach
  void tearDown() {
    hotArena.close();
    if (testScope.scope().isAlive()) {
      testScope.close();
    }
  }

  @Test
  void concurrentPut_uniqueKeys_allPresentAndReadable() throws Exception {
    int writerThreads = Math.max(4, Runtime.getRuntime().availableProcessors());
    int keysPerThread = 1_500;
    int totalKeys = writerThreads * keysPerThread;

    ExecutorService pool = Executors.newFixedThreadPool(writerThreads);
    CyclicBarrier start = new CyclicBarrier(writerThreads);

    List<Callable<Void>> tasks = new ArrayList<>();
    for (int t = 0; t < writerThreads; t++) {
      final int threadId = t;
      tasks.add(() -> {
        start.await();
        int base = threadId * keysPerThread;
        for (int i = 0; i < keysPerThread; i++) {
          int id = base + i;
          String key = "unique-k-" + id;
          skipList.insert(createKey(key), 1L, (byte) 1, createValue("value-" + id));
        }
        return null;
      });
    }

    runTasksWithTimeout(pool, tasks, 60);

    Set<String> seen = new HashSet<>();
    skipList.forEach(nodeOffset -> seen.add(readKey(nodeOffset)));

    assertEquals(totalKeys, seen.size(), "All unique inserts must be present in final structure");

    for (int i = 0; i < totalKeys; i++) {
      String key = "unique-k-" + i;
      MemorySegment keySeg = createKey(key);
      int off = skipList.get(keySeg, 1L);
      assertNotEquals(-1, off, "Missing key after concurrent insert: " + key);
      assertEquals(key, readKey(off), "Corrupted key bytes for key=" + key);
    }
  }

  @Test
  void concurrentPut_sameKey_manyWriters_noCorruptionAndAdmissibleResult() throws Exception {
    int writers = Math.max(8, Runtime.getRuntime().availableProcessors() * 2);
    String key = "hot-key";

    ExecutorService pool = Executors.newFixedThreadPool(writers + 1);
    CyclicBarrier start = new CyclicBarrier(writers + 1);
    Set<String> admissibleValues = ConcurrentHashMap.newKeySet();
    AtomicReference<Throwable> readerFailure = new AtomicReference<>();

    List<Callable<Void>> tasks = new ArrayList<>();
    for (int i = 0; i < writers; i++) {
      final int id = i;
      final String value = "writer-value-" + id;
      admissibleValues.add(value);
      tasks.add(() -> {
        start.await();
        skipList.insert(createKey(key), id + 1L, (byte) 1, createValue(value));
        return null;
      });
    }

    tasks.add(() -> {
      start.await();
      long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (System.nanoTime() < until) {
        MemorySegment keySeg = createKey(key);
        int off = skipList.get(keySeg, Long.MAX_VALUE);
        if (off == -1) {
          continue;
        }
        String observed = readValue(off);
        if (!admissibleValues.contains(observed)) {
          readerFailure.compareAndSet(null,
              new AssertionError("Observed out-of-domain value=" + observed));
          break;
        }
      }
      return null;
    });

    runTasksWithTimeout(pool, tasks, 30);
    assertNull(readerFailure.get(), "Reader observed corrupted value under contention");
    MemorySegment finalKeySeg = createKey(key);
    int finalOff = skipList.get(finalKeySeg, Long.MAX_VALUE);
    assertNotEquals(-1, finalOff, "Hot key must exist after writers complete");
    String finalValue = readValue(finalOff);
    assertTrue(admissibleValues.contains(finalValue),
        "Final value must be one of the concurrently inserted values, got=" + finalValue);
  }

  @Test
  void concurrentMixedReadWrite_noUnexpectedExceptions_and_eventualPresence() throws Exception {
    int writers = Math.max(4, Runtime.getRuntime().availableProcessors());
    int readers = Math.max(4, Runtime.getRuntime().availableProcessors());
    int keySpace = 4_000;
    int insertsPerWriter = 2_500;

    ExecutorService pool = Executors.newFixedThreadPool(writers + readers);
    CountDownLatch start = new CountDownLatch(1);
    AtomicBoolean stop = new AtomicBoolean(false);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    ConcurrentMap<String, Long> committed = new ConcurrentHashMap<>();

    List<Callable<Void>> tasks = new ArrayList<>();

    for (int i = 0; i < writers; i++) {
      final int writerId = i;
      tasks.add(() -> {
        start.await();
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        for (int j = 0; j < insertsPerWriter; j++) {
          int k = rnd.nextInt(keySpace);
          String key = "mix-k-" + k;
          long sn = ((long) writerId << 32) | j;
          String value = "mix-v-" + writerId + "-" + j;
          skipList.insert(createKey(key), sn, (byte) 1, createValue(value));
          committed.merge(key, sn, Math::max);
        }
        return null;
      });
    }

    for (int i = 0; i < readers; i++) {
      tasks.add(() -> {
        start.await();
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        while (!stop.get()) {
          String key = "mix-k-" + rnd.nextInt(keySpace);
          MemorySegment keySeg = createKey(key);
          int off = skipList.get(keySeg, Long.MAX_VALUE);
          if (off != -1) {
            readKey(off);
            readValue(off);
          }
        }
        return null;
      });
    }

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

    start.countDown();

    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
    for (int i = 0; i < writers; i++) {
      long left = deadlineNanos - System.nanoTime();
      if (left <= 0) {
        throw new TimeoutException("Timed out while waiting writer threads");
      }
      futures.get(i).get(left, TimeUnit.NANOSECONDS);
    }

    stop.set(true);
    for (int i = writers; i < futures.size(); i++) {
      long left = deadlineNanos - System.nanoTime();
      if (left <= 0) {
        throw new TimeoutException("Timed out while waiting reader threads");
      }
      futures.get(i).get(left, TimeUnit.NANOSECONDS);
    }

    pool.shutdownNow();
    assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS), "Executor did not terminate");

    assertNull(failure.get(), "Unexpected exception under mixed workload");

    for (String key : committed.keySet()) {
      MemorySegment keySeg = createKey(key);
      int off = skipList.get(keySeg, Long.MAX_VALUE);
      assertNotEquals(-1, off, "Committed key missing after mixed workload: " + key);
      assertEquals(key, readKey(off), "Key bytes corrupted for key=" + key);
    }
  }

  @RepeatedTest(15)
  @Tag("stress")
  void repeatedHotspotReadWriteRace_noInvalidReads() throws Exception {
    int threads = Math.max(8, Runtime.getRuntime().availableProcessors() * 2);
    int opsPerThread = 2_000;
    int hotKeys = 16;

    ExecutorService pool = Executors.newFixedThreadPool(threads);
    CyclicBarrier start = new CyclicBarrier(threads);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Set<String> validValues = ConcurrentHashMap.newKeySet();

    List<Callable<Void>> tasks = new ArrayList<>();
    for (int t = 0; t < threads; t++) {
      final int tid = t;
      tasks.add(() -> {
        start.await();
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        for (int i = 0; i < opsPerThread; i++) {
          String key = "hot-k-" + rnd.nextInt(hotKeys);
          if (rnd.nextInt(100) < 65) {
            long sn = ((long) tid << 32) | i;
            String value = "hot-v-" + tid + "-" + i;
            validValues.add(value);
            skipList.insert(createKey(key), sn, (byte) 1, createValue(value));
          } else {
            MemorySegment keySeg = createKey(key);
            int off = skipList.get(keySeg, Long.MAX_VALUE);
            if (off != -1) {
              String observedValue = readValue(off);
              if (!validValues.contains(observedValue)) {
                throw new AssertionError("Invalid value observed: " + observedValue + " for key=" + key);
              }
            }
          }
        }
        return null;
      });
    }

    try {
      runTasksWithTimeout(pool, tasks, 45);
    } catch (Throwable t) {
      failure.compareAndSet(null, t);
    }

    assertNull(failure.get(), "Hotspot race produced invalid observation or failure");
  }

  private void runTasksWithTimeout(ExecutorService pool, List<Callable<Void>> tasks, int seconds) throws Exception {
    List<Future<Void>> futures = pool.invokeAll(tasks);
    pool.shutdown();

    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
    for (Future<Void> f : futures) {
      long left = deadline - System.nanoTime();
      if (left <= 0) {
        throw new TimeoutException("Timeout waiting for task completion");
      }
      f.get(left, TimeUnit.NANOSECONDS);
    }

    assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS), "Executor did not terminate in time");
  }

  private MemorySegment createKey(String keyStr) {
    byte[] keyBytes = keyStr.getBytes(StandardCharsets.UTF_8);
    MemorySegment keySegment = testScope.allocate(keyBytes.length);
    keySegment.copyFrom(MemorySegment.ofArray(keyBytes));
    return keySegment;
  }

  private MemorySegment createValue(String valueStr) {
    byte[] valueBytes = valueStr.getBytes(StandardCharsets.UTF_8);
    MemorySegment valueSegment = testScope.allocate(valueBytes.length);
    valueSegment.copyFrom(MemorySegment.ofArray(valueBytes));
    return valueSegment;
  }

  private String readKey(int nodeOffset) {
    int coldOffset = hotArena.readInt(nodeOffset + 8 + 4);
    int keyLength = coldArena.readInt(coldOffset + KEY_LENGTH_OFFSET);
    int keyOffset = coldOffset + KEY_LENGTH_OFFSET + KEY_LENGTH + VALUE_LENGTH;
    MemorySegment keySegment = coldArena.readBytes(keyOffset, keyLength);
    return new String(keySegment.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
  }

  private String readValue(int nodeOffset) {
    int coldOffset = hotArena.readInt(nodeOffset + 8 + 4);
    int keyLength = coldArena.readInt(coldOffset + KEY_LENGTH_OFFSET);
    int valueLength = coldArena.readInt(coldOffset + KEY_LENGTH_OFFSET + KEY_LENGTH);
    int valueOffset = coldOffset + KEY_LENGTH_OFFSET + KEY_LENGTH + VALUE_LENGTH + keyLength;
    MemorySegment valueSegment = coldArena.readBytes(valueOffset, valueLength);
    return new String(valueSegment.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
  }
}

