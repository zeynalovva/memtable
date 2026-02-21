package az.zeynalov.memtable.benchmark;

import az.zeynalov.memtable.Arena;
import az.zeynalov.memtable.Footer;
import az.zeynalov.memtable.Header;
import az.zeynalov.memtable.SkipList;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;

@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1, jvmArgs = {"--enable-native-access=ALL-UNNAMED"})
public class SkipListBenchmark {

  // =========================================================================
  //  ARENA STATE
  // =========================================================================

  @State(Scope.Thread)
  public static class ArenaState {
    @Param({"1000", "10000", "100000"})
    int size;

    Arena arena;
    SkipList skipList;

    Header[] headersToInsert;
    Footer[] footersToInsert;
    Header[] headersToGet;
    Header missingHeader;
    int index;

    @Setup(Level.Trial)
    public void setup() {
      arena = new Arena();
      skipList = new SkipList(arena);
      skipList.init();

      headersToInsert = new Header[size];
      footersToInsert = new Footer[size];
      headersToGet = new Header[size];

      // Pre-create all data to avoid benchmarking String/Object allocation
      for (int i = 0; i < size; i++) {
        String keyStr = "key" + String.format("%07d", i);
        headersToInsert[i] = createHeader(keyStr, i);
        footersToInsert[i] = createFooter("val" + i);
        headersToGet[i] = createHeader(keyStr, i);
      }

      // Pre-fill the skip list for the GET benchmarks
      for (int i = 0; i < size; i++) {
        skipList.insert(headersToInsert[i], footersToInsert[i]);
      }

      missingHeader = createHeader("ZZZZZZZ_missing", 999999);
      index = 0;
    }

    @TearDown(Level.Trial)
    public void tearDown() {
      arena.close();
    }

    public int nextInsertIndex() {
      return index++ % size;
    }

    public Header nextHeader() {
      return headersToGet[index++ % size];
    }
  }

  // =========================================================================
  //  JDK STATE (Baseline)
  // =========================================================================

  @State(Scope.Thread)
  public static class JdkState {
    @Param({"1000", "10000", "100000"})
    int size;

    TreeMap<String, String> treeMap;
    ConcurrentSkipListMap<String, String> cslm;

    String[] keys;
    String missingKey = "ZZZZZZZ_missing";
    int index;

    @Setup(Level.Trial)
    public void setup() {
      treeMap = new TreeMap<>();
      cslm = new ConcurrentSkipListMap<>();
      keys = new String[size];

      for (int i = 0; i < size; i++) {
        keys[i] = "key" + String.format("%07d", i);
        treeMap.put(keys[i], "val" + i);
        cslm.put(keys[i], "val" + i);
      }
      index = 0;
    }

    public String nextKey() {
      return keys[index++ % size];
    }
  }

  // =========================================================================
  //  BENCHMARKS: INSERT
  // =========================================================================

  @Benchmark
  public void insert_arena(ArenaState state) {
    // Note: In a real test, you'd want to reset the arena
    // but for thrpt we just keep inserting.
    int idx = state.nextInsertIndex();
    state.skipList.insert(state.headersToInsert[idx], state.footersToInsert[idx]);
  }

  @Benchmark
  public void insert_treeMap(JdkState state) {
    state.treeMap.put(state.nextKey(), "value");
  }

  // =========================================================================
  //  BENCHMARKS: GET
  // =========================================================================

  @Benchmark
  public void get_arena(ArenaState state, Blackhole bh) {
    bh.consume(state.skipList.get(state.nextHeader()));
  }

  @Benchmark
  public void get_treeMap(JdkState state, Blackhole bh) {
    bh.consume(state.treeMap.get(state.nextKey()));
  }

  @Benchmark
  public void get_concurrentSkipListMap(JdkState state, Blackhole bh) {
    bh.consume(state.cslm.get(state.nextKey()));
  }

  // =========================================================================
  //  BENCHMARKS: GET MISS
  // =========================================================================

  @Benchmark
  public void getMiss_arena(ArenaState state, Blackhole bh) {
    bh.consume(state.skipList.get(state.missingHeader));
  }

  @Benchmark
  public void getMiss_treeMap(JdkState state, Blackhole bh) {
    bh.consume(state.treeMap.get(state.missingKey));
  }

  // =========================================================================
  //  BENCHMARKS: SCAN
  // =========================================================================

  @Benchmark
  public void scan_arena(ArenaState state, Blackhole bh) {
    state.skipList.forEach(bh::consume);
  }

  @Benchmark
  public void scan_treeMap(JdkState state, Blackhole bh) {
    state.treeMap.forEach((k, v) -> bh.consume(k));
  }

  // =========================================================================
  //  HELPERS
  // =========================================================================

  private static Header createHeader(String key, long sn) {
    byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
    return new Header(keyBytes.length, MemorySegment.ofArray(keyBytes), sn, (byte) 0);
  }

  private static Footer createFooter(String value) {
    byte[] valBytes = value.getBytes(StandardCharsets.UTF_8);
    return new Footer(valBytes.length, MemorySegment.ofArray(valBytes));
  }

  public static void main(String[] args) throws RunnerException {
    Options opts = new OptionsBuilder()
        .include(SkipListBenchmark.class.getSimpleName())
        .build();
    new Runner(opts).run();
  }
}