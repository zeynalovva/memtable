package az.zeynalov.memtable.benchmark;

import az.zeynalov.memtable.Arena;
import az.zeynalov.memtable.Footer;
import az.zeynalov.memtable.Header;
import az.zeynalov.memtable.SkipList;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
public class SkipListBenchmark {

  // ──────────────────────────────────────────────────────────
  //  State shared by all "get" benchmarks (pre-populated data)
  // ──────────────────────────────────────────────────────────
  @State(Scope.Benchmark)
  public static class ArenaGetState {
    @Param({"1000", "10000", "100000"})
    public int size;

    public Arena hotArena;
    public Arena coldArena;
    public SkipList skipList;
    public byte[][] keys;

    @Setup(Level.Trial)
    public void setup() {
      hotArena = new Arena();
      coldArena = new Arena();
      skipList = new SkipList(hotArena, coldArena);
      skipList.init();
      keys = new byte[size][];
      for (int i = 0; i < size; i++) {
        byte[] key = ("key-" + String.format("%08d", i)).getBytes(StandardCharsets.UTF_8);
        keys[i] = key;
        MemorySegment keySegment = MemorySegment.ofArray(key);
        byte[] val = ("value-" + i).getBytes(StandardCharsets.UTF_8);
        MemorySegment valSegment = MemorySegment.ofArray(val);
        Header header = new Header(key.length, keySegment, i, (byte) 0);
        Footer footer = new Footer(val.length, valSegment);
        skipList.insert(header, footer);
      }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
      hotArena.close();
    }
  }

  @State(Scope.Benchmark)
  public static class ConcurrentGetState {
    @Param({"1000", "10000", "100000"})
    public int size;

    public ConcurrentSkipListMap<String, String> map;
    public String[] keys;

    @Setup(Level.Trial)
    public void setup() {
      map = new ConcurrentSkipListMap<>();
      keys = new String[size];
      for (int i = 0; i < size; i++) {
        String key = "key-" + String.format("%08d", i);
        keys[i] = key;
        map.put(key, "value-" + i);
      }
    }
  }

  // ──────────────────────────────────────────────────────────
  //  State for "insert" benchmarks (arena cleared between invocations)
  // ──────────────────────────────────────────────────────────
  @State(Scope.Thread)
  public static class ArenaInsertState {
    private static final int BATCH_SIZE = 5000;

    public Arena hotArena;
    public Arena coldArena;
    public SkipList skipList;
    public Header[] headers;
    public Footer[] footers;

    @Setup(Level.Invocation)
    public void setup() {
      hotArena = new Arena();
      coldArena = new Arena();
      skipList = new SkipList(hotArena, coldArena);
      skipList.init();
      headers = new Header[BATCH_SIZE];
      footers = new Footer[BATCH_SIZE];
      for (int i = 0; i < BATCH_SIZE; i++) {
        byte[] key = ("key-" + String.format("%08d", i)).getBytes(StandardCharsets.UTF_8);
        byte[] val = ("value-" + i).getBytes(StandardCharsets.UTF_8);
        MemorySegment keySegment = MemorySegment.ofArray(key);
        MemorySegment valSegment = MemorySegment.ofArray(val);
        headers[i] = new Header(key.length, keySegment, i, (byte) 0);
        footers[i] = new Footer(val.length, valSegment);
      }
    }

    @TearDown(Level.Invocation)
    public void tearDown() {
      hotArena.close();
    }
  }

  @State(Scope.Thread)
  public static class ConcurrentInsertState {
    private static final int BATCH_SIZE = 5000;

    public ConcurrentSkipListMap<String, String> map;
    public String[] keys;
    public String[] values;

    @Setup(Level.Invocation)
    public void setup() {
      map = new ConcurrentSkipListMap<>();
      keys = new String[BATCH_SIZE];
      values = new String[BATCH_SIZE];
      for (int i = 0; i < BATCH_SIZE; i++) {
        keys[i] = "key-" + String.format("%08d", i);
        values[i] = "value-" + i;
      }
    }
  }

  // ──────────────────────────────────────────────────────────
  //  Thread-local index state for get benchmarks
  // ──────────────────────────────────────────────────────────
  @State(Scope.Thread)
  public static class IndexState {
    public int index;

    @Setup(Level.Invocation)
    public void setup(ArenaGetState state) {
      index = ThreadLocalRandom.current().nextInt(state.size);
    }
  }

  @State(Scope.Thread)
  public static class ConcurrentIndexState {
    public int index;

    @Setup(Level.Invocation)
    public void setup(ConcurrentGetState state) {
      index = ThreadLocalRandom.current().nextInt(state.size);
    }
  }

  // ──────────────────────────────────────────────────────────
  //  GET benchmarks – hit (key exists)
  // ──────────────────────────────────────────────────────────
  @Benchmark
  public int get_arena(ArenaGetState state, IndexState idx) {
    byte[] key = state.keys[idx.index];
    MemorySegment keySegment = MemorySegment.ofArray(key);
    Header header = new Header(key.length, keySegment, idx.index, (byte) 0);
    return state.skipList.get(header);
  }

  @Benchmark
  public String get_concurrentSkipListMap(ConcurrentGetState state, ConcurrentIndexState idx) {
    return state.map.get(state.keys[idx.index]);
  }

  // ──────────────────────────────────────────────────────────
  //  GET benchmarks – miss (key does NOT exist)
  // ──────────────────────────────────────────────────────────
  @Benchmark
  public int getMiss_arena(ArenaGetState state) {
    byte[] key = "nonexistent-key-999999".getBytes(StandardCharsets.UTF_8);
    MemorySegment keySegment = MemorySegment.ofArray(key);
    Header header = new Header(key.length, keySegment, 0, (byte) 0);
    return state.skipList.get(header);
  }

  @Benchmark
  public String getMiss_concurrentSkipListMap(ConcurrentGetState state) {
    return state.map.get("nonexistent-key-999999");
  }

  // ──────────────────────────────────────────────────────────
  //  INSERT benchmarks – batch insert into fresh structure
  // ──────────────────────────────────────────────────────────
  @Benchmark
  @OperationsPerInvocation(5000)
  public void insert_arena(ArenaInsertState state) {
    for (int i = 0; i < ArenaInsertState.BATCH_SIZE; i++) {
      state.skipList.insert(state.headers[i], state.footers[i]);
    }
  }

  @Benchmark
  @OperationsPerInvocation(5000)
  public void insert_concurrentSkipListMap(ConcurrentInsertState state) {
    for (int i = 0; i < ConcurrentInsertState.BATCH_SIZE; i++) {
      state.map.put(state.keys[i], state.values[i]);
    }
  }

  // ──────────────────────────────────────────────────────────
  //  Main – convenience runner
  // ──────────────────────────────────────────────────────────
  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(SkipListBenchmark.class.getSimpleName())
        .build();
    new Runner(opt).run();
  }
}

