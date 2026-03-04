package az.zeynalov.memtable.benchmark;

import az.zeynalov.memtable.Arena;
import az.zeynalov.memtable.Footer;
import az.zeynalov.memtable.Header;
import az.zeynalov.memtable.SkipList;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;


@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 7, time = 2)
@Fork(
    value = 1,
    jvmArgsAppend = {
        "-XX:+UseG1GC",
        "-Xms512m",
        "-Xmx512m"
    }
)
public class SkipListBenchmark {

  // ─────────────────────────────────────────────────────────
  //  Shared key generation — identical keys for both sides
  // ───────────────────────────────��─────────────────────────

  /**
   * Generates a fixed-width, lexicographically sortable key.
   * "key-00000042" — zero-padded so byte-order == lexicographic order.
   * Both CSLM (with Arrays.compareUnsigned) and arena skiplist
   * will see identical ordering.
   */
  private static byte[] makeKey(int i) {
    return String.format("key-%010d", i).getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] makeValue(int i) {
    return String.format("val-%010d", i).getBytes(StandardCharsets.UTF_8);
  }

  // ─────────────────────────────────────────────────────────
  //  GET / SCAN state  (pre-populated, Trial-scoped)
  // ─────────────────────────────────────────────────────────

  @State(Scope.Benchmark)
  public static class ArenaGetState {

    @Param({"1000", "10000", "100000"})
    public int size;

    public Arena hotArena;
    public Arena coldArena;
    public SkipList skipList;

    // Pre-built hit headers — zero allocation inside @Benchmark
    public Header[] hitHeaders;
    // Pre-built miss header — key is guaranteed absent
    public Header missHeader;
    // Pre-built scan-start header — key at the 25th percentile
    public Header scanStartHeader;

    @Setup(Level.Trial)
    public void setup() {
      hotArena  = new Arena();
      coldArena = new Arena();
      skipList  = new SkipList(hotArena, coldArena);
      skipList.init();

      hitHeaders = new Header[size];

      for (int i = 0; i < size; i++) {
        byte[] key = makeKey(i);
        byte[] val = makeValue(i);
        MemorySegment keySeg = MemorySegment.ofArray(key);
        MemorySegment valSeg = MemorySegment.ofArray(val);
        // SN = Long.MAX_VALUE → always returns the latest version (MVCC hit)
        Header h = new Header(key.length, keySeg, Long.MAX_VALUE, (byte) 0);
        Footer f = new Footer(val.length, valSeg);
        skipList.insert(h, f);
        // Re-use same MemorySegment for lookup — it's read-only, safe
        hitHeaders[i] = new Header(key.length, keySeg, Long.MAX_VALUE, (byte) 0);
      }

      // Miss key: lexicographically outside the inserted range
      byte[] missKey = "key-9999999999".getBytes(StandardCharsets.UTF_8);
      missHeader = new Header(
          missKey.length,
          MemorySegment.ofArray(missKey),
          Long.MAX_VALUE,
          (byte) 0
      );

      // Scan start: 25th percentile key
      byte[] scanKey = makeKey(size / 4);
      scanStartHeader = new Header(
          scanKey.length,
          MemorySegment.ofArray(scanKey),
          Long.MAX_VALUE,
          (byte) 0
      );
    }

    @TearDown(Level.Trial)
    public void tearDown() {
      hotArena.close();
      coldArena.close();
    }
  }

  @State(Scope.Benchmark)
  public static class CslmGetState {

    @Param({"1000", "10000", "100000"})
    public int size;

    // byte[] comparator — matches arena's unsigned lexicographic comparison exactly
    public ConcurrentSkipListMap<byte[], byte[]> map;

    public byte[][] hitKeys;
    public byte[] missKey;
    public byte[] scanStartKey;

    @Setup(Level.Trial)
    public void setup() {
      // Arrays.compareUnsigned mirrors MemorySegment.mismatch byte semantics
      map = new ConcurrentSkipListMap<>(Arrays::compareUnsigned);
      hitKeys = new byte[size][];

      for (int i = 0; i < size; i++) {
        byte[] key = makeKey(i);
        byte[] val = makeValue(i);
        hitKeys[i] = key;
        map.put(key, val);
      }

      missKey      = "key-9999999999".getBytes(StandardCharsets.UTF_8);
      scanStartKey = makeKey(size / 4);
    }
  }

  // ─────────────────────────────────────────────────────────
  //  INSERT state  (fresh structure per invocation)
  // ─────────────────────────────────────────────────────────

  @State(Scope.Thread)
  public static class ArenaInsertState {
    static final int BATCH = 5_000;

    public Arena    hotArena;
    public Arena    coldArena;
    public SkipList skipList;
    public Header[] headers;
    public Footer[] footers;

    @Setup(Level.Invocation)
    public void setup() {
      hotArena  = new Arena();
      coldArena = new Arena();
      skipList  = new SkipList(hotArena, coldArena);
      skipList.init();
      headers = new Header[BATCH];
      footers = new Footer[BATCH];
      for (int i = 0; i < BATCH; i++) {
        byte[] key = makeKey(i);
        byte[] val = makeValue(i);
        // MemorySegment.ofArray is fine here — it is part of insert work,
        // symmetric with CSLM which also receives the byte[] directly.
        headers[i] = new Header(key.length, MemorySegment.ofArray(key), i, (byte) 0);
        footers[i] = new Footer(val.length, MemorySegment.ofArray(val));
      }
    }

    @TearDown(Level.Invocation)
    public void tearDown() {
      hotArena.close();
      coldArena.close();
    }
  }

  @State(Scope.Thread)
  public static class CslmInsertState {
    static final int BATCH = 5_000;

    public ConcurrentSkipListMap<byte[], byte[]> map;
    public byte[][] keys;
    public byte[][] values;

    @Setup(Level.Invocation)
    public void setup() {
      map    = new ConcurrentSkipListMap<>(Arrays::compareUnsigned);
      keys   = new byte[BATCH][];
      values = new byte[BATCH][];
      for (int i = 0; i < BATCH; i++) {
        keys[i]   = makeKey(i);
        values[i] = makeValue(i);
      }
    }
  }

  // ─────────────────────────────────────────────────────────
  //  Thread-local random index — Level.Iteration avoids
  //  per-call setup overhead that biased the old benchmark
  // ─────────────────────────────────────────────────────────

  @State(Scope.Thread)
  public static class RandomIndex {
    // Rotates through a pre-shuffled index array — avoids
    // ThreadLocalRandom.nextInt() overhead inside the benchmark method
    // while still covering the full key space randomly.
    private int[]  indices;
    private int    cursor;

    @Setup(Level.Iteration)
    public void setup(@SuppressWarnings("unused") ArenaGetState s) {
      indices = buildShuffled(s.size);
      cursor  = 0;
    }

    public int next() {
      if (cursor >= indices.length) cursor = 0;
      return indices[cursor++];
    }

    private static int[] buildShuffled(int n) {
      int[] a = new int[n];
      for (int i = 0; i < n; i++) a[i] = i;
      // Fisher-Yates
      ThreadLocalRandom rng = ThreadLocalRandom.current();
      for (int i = n - 1; i > 0; i--) {
        int j = rng.nextInt(i + 1);
        int t = a[i]; a[i] = a[j]; a[j] = t;
      }
      return a;
    }
  }

  @State(Scope.Thread)
  public static class CslmRandomIndex {
    private int[] indices;
    private int   cursor;

    @Setup(Level.Iteration)
    public void setup(CslmGetState s) {
      indices = new int[s.size];
      for (int i = 0; i < s.size; i++) indices[i] = i;
      ThreadLocalRandom rng = ThreadLocalRandom.current();
      for (int i = s.size - 1; i > 0; i--) {
        int j = rng.nextInt(i + 1);
        int t = indices[i]; indices[i] = indices[j]; indices[j] = t;
      }
      cursor = 0;
    }

    public int next() {
      if (cursor >= indices.length) cursor = 0;
      return indices[cursor++];
    }
  }

  // ─────────────────────────────────────────────────────────
  //  1. GET HIT
  //     Both sides look up a key that is guaranteed to exist.
  //     Arena: SN=MAX_VALUE → returns the latest (and only) version.
  //     CSLM:  direct byte[] key lookup.
  // ─────────────────────────────────────────────────────────

  @Benchmark
  public int getHit_arena(ArenaGetState s, RandomIndex idx) {
    return s.skipList.get(s.hitHeaders[idx.next()]);
  }

  @Benchmark
  public byte[] getHit_cslm(CslmGetState s, CslmRandomIndex idx) {
    return s.map.get(s.hitKeys[idx.next()]);
  }

  // ─────────────────────────────────────────────────────────
  //  2. GET MISS
  //     Key is absent in both structures.
  //     Pre-built — zero allocation in the benchmark method.
  // ───────────────────���─────────────────────────────────────

  @Benchmark
  public int getMiss_arena(ArenaGetState s) {
    return s.skipList.get(s.missHeader);
  }

  @Benchmark
  public byte[] getMiss_cslm(CslmGetState s) {
    return s.map.get(s.missKey);
  }

  // ─────────────────────────────────────────────────────────
  //  3. SCAN
  //     Iterate from the 25th-percentile key to the end,
  //     consuming every offset/value so the JIT cannot elide
  //     the work. Both sides visit the same number of entries.
  //
  //     Arena: forEach from level-0 linked list.
  //     CSLM:  tailMap().forEach() — same linear scan.
  // ─────────────────────────────────────────────────────────

  @Benchmark
  public int scan_arena(ArenaGetState s, Blackhole bh) {
    // forEach visits every node in ascending order from the beginning.
    // We count how many are >= scan start by consuming via Blackhole.
    // This exercises the level-0 linked-list traversal — the hot path
    // for SSTable flush / range queries.
    int[] count = {0};
    s.skipList.forEach(offset -> {
      bh.consume(offset);
      count[0]++;
    });
    return count[0];
  }

  @Benchmark
  public int scan_cslm(CslmGetState s, Blackhole bh) {
    // tailMap gives us all entries from the 25th-percentile key onward.
    // We iterate and consume each entry so the JIT cannot elide the loop.
    int count = 0;
    for (byte[] v : s.map.tailMap(s.scanStartKey).values()) {
      bh.consume(v);
      count++;
    }
    return count;
  }

  // ─────────────────────────────────────────────────────────
  //  4. INSERT (batch)
  //     Fresh structure per invocation so neither side has
  //     the advantage of a warm, already-indexed structure.
  //     @OperationsPerInvocation normalises to per-insert cost.
  // ─────────────────────────────────────────────────────────

  @Benchmark
  @OperationsPerInvocation(ArenaInsertState.BATCH)
  public void insert_arena(ArenaInsertState s) {
    for (int i = 0; i < ArenaInsertState.BATCH; i++) {
      s.skipList.insert(s.headers[i], s.footers[i]);
    }
  }

  @Benchmark
  @OperationsPerInvocation(CslmInsertState.BATCH)
  public void insert_cslm(CslmInsertState s) {
    for (int i = 0; i < CslmInsertState.BATCH; i++) {
      s.map.put(s.keys[i], s.values[i]);
    }
  }

  // ─────────────────────────────────────────────────────────
  //  5. MIXED  (80% get, 20% insert)
  //     Simulates real memtable workload: many reads,
  //     occasional writes. Uses a shared AtomicInteger counter
  //     so the ratio is approximate but deterministic enough
  //     for benchmarking purposes.
  //
  //     This is the benchmark that matters most for a DBMS.
  // ─────────────────────────────────────────────────────────

  @State(Scope.Benchmark)
  public static class ArenaMixedState {
    @Param({"10000"})
    public int size;

    public Arena    hotArena;
    public Arena    coldArena;
    public SkipList skipList;
    public Header[] hitHeaders;
    public Header[] insertHeaders;
    public Footer[] insertFooters;
    public AtomicInteger insertCounter = new AtomicInteger(0);

    @Setup(Level.Trial)
    public void setup() {
      hotArena  = new Arena();
      coldArena = new Arena();
      skipList  = new SkipList(hotArena, coldArena);
      skipList.init();
      hitHeaders    = new Header[size];
      insertHeaders = new Header[size];
      insertFooters = new Footer[size];

      for (int i = 0; i < size; i++) {
        byte[] key = makeKey(i);
        byte[] val = makeValue(i);
        MemorySegment keySeg = MemorySegment.ofArray(key);
        MemorySegment valSeg = MemorySegment.ofArray(val);
        skipList.insert(
            new Header(key.length, keySeg, i, (byte) 0),
            new Footer(val.length, valSeg)
        );
        hitHeaders[i]    = new Header(key.length, keySeg, Long.MAX_VALUE, (byte) 0);
        insertHeaders[i] = new Header(key.length, keySeg, size + i, (byte) 0);
        insertFooters[i] = new Footer(val.length, valSeg);
      }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
      hotArena.close();
      coldArena.close();
    }
  }

  @State(Scope.Benchmark)
  public static class CslmMixedState {
    @Param({"10000"})
    public int size;

    public ConcurrentSkipListMap<byte[], byte[]> map;
    public byte[][] hitKeys;
    public byte[][] insertKeys;
    public byte[][] insertValues;
    public AtomicInteger insertCounter = new AtomicInteger(0);

    @Setup(Level.Trial)
    public void setup() {
      map          = new ConcurrentSkipListMap<>(Arrays::compareUnsigned);
      hitKeys      = new byte[size][];
      insertKeys   = new byte[size][];
      insertValues = new byte[size][];

      for (int i = 0; i < size; i++) {
        byte[] key = makeKey(i);
        byte[] val = makeValue(i);
        hitKeys[i]      = key;
        insertKeys[i]   = makeKey(size + i);
        insertValues[i] = makeValue(size + i);
        map.put(key, val);
      }
    }
  }

  @Benchmark
  public int mixed_arena(ArenaMixedState s, RandomIndex idx) {
    int i = idx.next();
    // 80/20 split based on index position in the shuffled array
    if ((i & 0x7) != 0) {
      // 87.5% reads (7 out of 8)
      return s.skipList.get(s.hitHeaders[i % s.size]);
    } else {
      // 12.5% writes — cycles through insert headers
      int wi = s.insertCounter.getAndIncrement() % s.size;
      s.skipList.insert(s.insertHeaders[wi], s.insertFooters[wi]);
      return wi;
    }
  }

  @Benchmark
  public int mixed_cslm(CslmMixedState s, CslmRandomIndex idx) {
    int i = idx.next();
    if ((i & 0x7) != 0) {
      byte[] v = s.map.get(s.hitKeys[i % s.size]);
      return v == null ? -1 : v.length;
    } else {
      int wi = s.insertCounter.getAndIncrement() % s.size;
      s.map.put(s.insertKeys[wi], s.insertValues[wi]);
      return wi;
    }
  }

  // ─────────────────────────────────────────────────────────
  //  Main
  // ─────────────────────────────────────────────────────────

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(SkipListBenchmark.class.getSimpleName())
        .build();
    new Runner(opt).run();
  }
}