package az.zeynalov.memtable.benchmark;

import az.zeynalov.memtable.ArenaImpl;
import az.zeynalov.memtable.Footer;
import az.zeynalov.memtable.Header;
import az.zeynalov.memtable.Record;
import az.zeynalov.memtable.SkipList;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks for SkipList with baseline comparisons against
 * java.util.TreeMap and java.util.concurrent.ConcurrentSkipListMap.
 *
 * Without a baseline, raw numbers are meaningless.
 * This benchmark answers: "Is my implementation fast compared to the JDK?"
 *
 * Run:
 *   mvn clean package -DskipTests
 *   java --enable-native-access=ALL-UNNAMED -jar target/benchmarks.jar
 *
 * Quick run (shorter, less accurate):
 *   java --enable-native-access=ALL-UNNAMED -jar target/benchmarks.jar -wi 2 -i 3 -f 1
 */
@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1, jvmArgs = {"--enable-native-access=ALL-UNNAMED"})
public class SkipListBenchmark {

  // =========================================================================
  //  State: Arena SkipList
  // =========================================================================

  @State(Scope.Thread)
  public static class ArenaInsertState {
    ArenaImpl arena;
    SkipList skipList;
    int counter;

    @Setup(Level.Invocation)
    public void setup() {
      arena = new ArenaImpl();
      skipList = new SkipList(arena);
      skipList.init();
      counter = 0;
    }

    @TearDown(Level.Invocation)
    public void tearDown() {
      arena.close();
    }
  }

  @State(Scope.Thread)
  public static class ArenaReadState {
    @Param({"1000", "10000", "100000"})
    int size;

    ArenaImpl arena;
    SkipList skipList;
    Header[] searchHeaders;
    int index;

    @Setup(Level.Trial)
    public void setup() {
      arena = new ArenaImpl();
      skipList = new SkipList(arena);
      skipList.init();
      searchHeaders = new Header[size];

      for (int i = 0; i < size; i++) {
        String key = "key" + String.format("%07d", i);
        skipList.insert(createRecord(key, i, "value" + i));
        searchHeaders[i] = createHeader(key, i);
      }
      index = 0;
    }

    @TearDown(Level.Trial)
    public void tearDown() {
      arena.close();
    }

    public Header nextSearchHeader() {
      Header h = searchHeaders[index];
      index = (index + 1) % size;
      return h;
    }
  }

  @State(Scope.Thread)
  public static class ArenaScanState {
    @Param({"1000", "10000"})
    int size;

    ArenaImpl arena;
    SkipList skipList;

    @Setup(Level.Trial)
    public void setup() {
      arena = new ArenaImpl();
      skipList = new SkipList(arena);
      skipList.init();

      for (int i = 0; i < size; i++) {
        String key = "key" + String.format("%07d", i);
        skipList.insert(createRecord(key, i, "val" + i));
      }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
      arena.close();
    }
  }

  // =========================================================================
  //  State: JDK TreeMap baseline
  // =========================================================================

  @State(Scope.Thread)
  public static class TreeMapInsertState {
    TreeMap<String, String> map;
    int counter;

    @Setup(Level.Invocation)
    public void setup() {
      map = new TreeMap<>();
      counter = 0;
    }
  }

  @State(Scope.Thread)
  public static class TreeMapReadState {
    @Param({"1000", "10000", "100000"})
    int size;

    TreeMap<String, String> map;
    String[] keys;
    int index;

    @Setup(Level.Trial)
    public void setup() {
      map = new TreeMap<>();
      keys = new String[size];

      for (int i = 0; i < size; i++) {
        String key = "key" + String.format("%07d", i);
        map.put(key, "value" + i);
        keys[i] = key;
      }
      index = 0;
    }

    public String nextKey() {
      String k = keys[index];
      index = (index + 1) % size;
      return k;
    }
  }

  @State(Scope.Thread)
  public static class TreeMapScanState {
    @Param({"1000", "10000"})
    int size;

    TreeMap<String, String> map;

    @Setup(Level.Trial)
    public void setup() {
      map = new TreeMap<>();
      for (int i = 0; i < size; i++) {
        String key = "key" + String.format("%07d", i);
        map.put(key, "val" + i);
      }
    }
  }

  // =========================================================================
  //  State: JDK ConcurrentSkipListMap baseline
  // =========================================================================

  @State(Scope.Thread)
  public static class CslmInsertState {
    ConcurrentSkipListMap<String, String> map;
    int counter;

    @Setup(Level.Invocation)
    public void setup() {
      map = new ConcurrentSkipListMap<>();
      counter = 0;
    }
  }

  @State(Scope.Thread)
  public static class CslmReadState {
    @Param({"1000", "10000", "100000"})
    int size;

    ConcurrentSkipListMap<String, String> map;
    String[] keys;
    int index;

    @Setup(Level.Trial)
    public void setup() {
      map = new ConcurrentSkipListMap<>();
      keys = new String[size];

      for (int i = 0; i < size; i++) {
        String key = "key" + String.format("%07d", i);
        map.put(key, "value" + i);
        keys[i] = key;
      }
      index = 0;
    }

    public String nextKey() {
      String k = keys[index];
      index = (index + 1) % size;
      return k;
    }
  }

  // =========================================================================
  //  Helpers
  // =========================================================================

  private static Header createHeader(String key, long sn) {
    byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
    return new Header(keyBytes.length, MemorySegment.ofArray(keyBytes), sn, (byte) 0);
  }

  private static Record createRecord(String key, long sn, String value) {
    byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
    byte[] valBytes = value.getBytes(StandardCharsets.UTF_8);
    Header header = new Header(keyBytes.length, MemorySegment.ofArray(keyBytes), sn, (byte) 0);
    Footer footer = new Footer(valBytes.length, MemorySegment.ofArray(valBytes));
    return new Record(header, footer);
  }

  // =========================================================================
  //  INSERT benchmarks
  // =========================================================================

  @Benchmark
  public void insert_arena(ArenaInsertState state) {
    int i = state.counter++;
    String key = "key" + String.format("%07d", i);
    state.skipList.insert(createRecord(key, i, "value" + i));
  }

  @Benchmark
  public void insert_treeMap(TreeMapInsertState state) {
    int i = state.counter++;
    String key = "key" + String.format("%07d", i);
    state.map.put(key, "value" + i);
  }

  @Benchmark
  public void insert_concurrentSkipListMap(CslmInsertState state) {
    int i = state.counter++;
    String key = "key" + String.format("%07d", i);
    state.map.put(key, "value" + i);
  }

  // =========================================================================
  //  GET (point lookup) benchmarks
  // =========================================================================

  @Benchmark
  public Record get_arena(ArenaReadState state) {
    return state.skipList.get(state.nextSearchHeader());
  }

  @Benchmark
  public String get_treeMap(TreeMapReadState state) {
    return state.map.get(state.nextKey());
  }

  @Benchmark
  public String get_concurrentSkipListMap(CslmReadState state) {
    return state.map.get(state.nextKey());
  }

  // =========================================================================
  //  SCAN (forEach / iterate all) benchmarks
  // =========================================================================

  @Benchmark
  public int scan_arena(ArenaScanState state) {
    int[] count = {0};
    state.skipList.forEach(header -> count[0]++);
    return count[0];
  }

  @Benchmark
  public int scan_treeMap(TreeMapScanState state) {
    int[] count = {0};
    state.map.forEach((k, v) -> count[0]++);
    return count[0];
  }

  // =========================================================================
  //  GET miss (key not found) benchmarks
  // =========================================================================

  @Benchmark
  public Record getMiss_arena(ArenaReadState state) {
    return state.skipList.get(createHeader("ZZZZZZZZ_missing", 999999));
  }

  @Benchmark
  public String getMiss_treeMap(TreeMapReadState state) {
    return state.map.get("ZZZZZZZZ_missing");
  }

  @Benchmark
  public String getMiss_concurrentSkipListMap(CslmReadState state) {
    return state.map.get("ZZZZZZZZ_missing");
  }

  // ---- Main ----

  public static void main(String[] args) throws RunnerException {
    Options opts = new OptionsBuilder()
        .include(SkipListBenchmark.class.getSimpleName())
        .build();
    new Runner(opts).run();
  }
}

