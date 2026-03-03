package az.zeynalov.memtable;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;

/**
 * Standalone runner to observe JIT inlining decisions on SkipList.get().
 *
 * Run directly with:
 *
 *   java -XX:+UnlockDiagnosticVMOptions \
 *        -XX:+PrintInlining \
 *        -XX:+PrintCompilation \
 *        -cp target/classes \
 *        az.zeynalov.memtable.InliningTest 2>&1 | grep -i "skiplist"
 *
 * Or to save full output to a file:
 *
 *   java -XX:+UnlockDiagnosticVMOptions \
 *        -XX:+PrintInlining \
 *        -XX:+PrintCompilation \
 *        -cp target/classes \
 *        az.zeynalov.memtable.InliningTest 2>&1 | tee /tmp/jit.txt
 *
 *   grep -i "SkipList" /tmp/jit.txt
 *
 * Alternative: use LogCompilation to write XML (viewable with JITWatch):
 *
 *   java -XX:+UnlockDiagnosticVMOptions \
 *        -XX:+LogCompilation \
 *        -XX:LogFile=/tmp/jit_log.xml \
 *        -cp target/classes \
 *        az.zeynalov.memtable.InliningTest
 */
public class InliningTest {

  public static void main(String[] args) {
    Arena hotArena = new Arena();
    Arena coldArena = new Arena();
    SkipList skipList = new SkipList(hotArena, coldArena);
    skipList.init();

    try (java.lang.foreign.Arena memScope = java.lang.foreign.Arena.ofShared()) {
      int insertCount = 10_000;
      int getIterations = 500;

      // Pre-build headers and footers
      Header[] insertHeaders = new Header[insertCount];
      Footer[] insertFooters = new Footer[insertCount];
      for (int i = 0; i < insertCount; i++) {
        String key = "key_" + String.format("%06d", i);
        insertHeaders[i] = makeHeader(memScope, key, i, (byte) 1);
        insertFooters[i] = makeFooter(memScope, "val_" + i);
      }

      // Bulk insert
      for (int i = 0; i < insertCount; i++) {
        skipList.insert(insertHeaders[i], insertFooters[i]);
      }
      System.out.println("Inserted " + insertCount + " keys");

      // Pre-build query headers
      Header[] queryHeaders = new Header[insertCount];
      for (int i = 0; i < insertCount; i++) {
        String key = "key_" + String.format("%06d", i);
        queryHeaders[i] = makeHeader(memScope, key, i, (byte) 1);
      }

      // Tight hot loop: 10k keys x 500 iterations = 5M get() calls
      int found = 0;
      long start = System.nanoTime();
      for (int iter = 0; iter < getIterations; iter++) {
        for (int i = 0; i < insertCount; i++) {
          int offset = skipList.get(queryHeaders[i]);
          if (offset != -1) {
            found++;
          }
        }
      }
      long elapsed = System.nanoTime() - start;

      System.out.println("Found: " + found + " / " + ((long) insertCount * getIterations));
      System.out.println("Time: " + (elapsed / 1_000_000) + " ms");
      System.out.println("Avg get(): " + (elapsed / ((long) insertCount * getIterations)) + " ns");

    } finally {
      hotArena.close();
    }
  }

  private static Header makeHeader(java.lang.foreign.Arena scope, String keyStr, long SN,
      byte type) {
    byte[] keyBytes = keyStr.getBytes(StandardCharsets.UTF_8);
    MemorySegment keySegment = scope.allocate(keyBytes.length);
    keySegment.copyFrom(MemorySegment.ofArray(keyBytes));
    return new Header(keyBytes.length, keySegment, SN, type);
  }

  private static Footer makeFooter(java.lang.foreign.Arena scope, String valueStr) {
    byte[] valueBytes = valueStr.getBytes(StandardCharsets.UTF_8);
    MemorySegment valueSegment = scope.allocate(valueBytes.length);
    valueSegment.copyFrom(MemorySegment.ofArray(valueBytes));
    return new Footer(valueBytes.length, valueSegment);
  }
}

