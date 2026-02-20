package az.zeynalov.tests;

import az.zeynalov.memtable.ArenaImpl;
import az.zeynalov.memtable.Header;
import az.zeynalov.memtable.SkipList;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class SkipListTest {

  private static final byte TYPE_PUT = (byte) 0;
  private static final byte TYPE_DELETE = (byte) 1;

  private ArenaImpl arena;
  private SkipList skipList;

  @BeforeEach
  void setUp() {
    arena = new ArenaImpl();
    skipList = new SkipList(arena);
    skipList.init();
  }

  @AfterEach
  void tearDown() {
    arena.close();
  }

  private Header createHeader(String key, long sn) {
    return createHeader(key, sn, TYPE_PUT);
  }

  private Header createHeader(String key, long sn, byte type) {
    byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
    MemorySegment keySegment = MemorySegment.ofArray(keyBytes);
    return new Header(keyBytes.length, keySegment, sn, type);
  }

  @Test
  void testInsertAndGetSingleElement() {
    Header header = createHeader("key1", 1);
    skipList.insert(header);

    Header searchHeader = createHeader("key1", 1);
    Header result = skipList.get(searchHeader);

    assertNotNull(result);
    assertEquals(1, result.SN());
  }

  @Test
  void testGetNonExistentKey() {
    Header header = createHeader("key1", 1);
    skipList.insert(header);

    Header searchHeader = createHeader("key2", 1);
    Header result = skipList.get(searchHeader);

    assertNull(result);
  }

  @Test
  void testInsertMultipleElements() {
    skipList.insert(createHeader("apple", 1));
    skipList.insert(createHeader("banana", 2));
    skipList.insert(createHeader("cherry", 3));

    assertNotNull(skipList.get(createHeader("apple", 1)));
    assertNotNull(skipList.get(createHeader("banana", 2)));
    assertNotNull(skipList.get(createHeader("cherry", 3)));
  }

  @Test
  void testInsertDuplicateKeyAndSN() {
    skipList.insert(createHeader("key1", 1));
    skipList.insert(createHeader("key1", 1));

    Header result = skipList.get(createHeader("key1", 1));

    assertNotNull(result);
    assertEquals(1, result.SN());
  }

  @Test
  void testSameKeyDifferentSNDescendingOrder() {
    skipList.insert(createHeader("key1", 1));
    skipList.insert(createHeader("key1", 3));
    skipList.insert(createHeader("key1", 2));

    Header result1 = skipList.get(createHeader("key1", 3));
    Header result2 = skipList.get(createHeader("key1", 2));
    Header result3 = skipList.get(createHeader("key1", 1));

    assertNotNull(result1);
    assertEquals(3, result1.SN());
    assertNotNull(result2);
    assertEquals(2, result2.SN());
    assertNotNull(result3);
    assertEquals(1, result3.SN());
  }

  @Test
  void testInsertInReverseOrder() {
    skipList.insert(createHeader("z", 1));
    skipList.insert(createHeader("m", 2));
    skipList.insert(createHeader("a", 3));

    assertEquals(3, skipList.get(createHeader("a", 3)).SN());
    assertEquals(2, skipList.get(createHeader("m", 2)).SN());
    assertEquals(1, skipList.get(createHeader("z", 1)).SN());
  }

  @Test
  void testCompareHeadersEqualKeyAndSN() {
    Header a = createHeader("test", 1);
    Header b = createHeader("test", 1);

    assertEquals(0, skipList.compareHeaders(a, b));
  }

  @Test
  void testCompareHeadersKeyLessThan() {
    Header a = createHeader("abc", 1);
    Header b = createHeader("abd", 1);

    assertTrue(skipList.compareHeaders(a, b) < 0);
  }

  @Test
  void testCompareHeadersKeyGreaterThan() {
    Header a = createHeader("xyz", 1);
    Header b = createHeader("abc", 1);

    assertTrue(skipList.compareHeaders(a, b) > 0);
  }

  @Test
  void testCompareHeadersSameKeySNDescending() {
    Header a = createHeader("test", 3);
    Header b = createHeader("test", 1);

    assertTrue(skipList.compareHeaders(a, b) < 0);
  }

  @Test
  void testCompareHeadersSameKeyLowerSN() {
    Header a = createHeader("test", 1);
    Header b = createHeader("test", 3);

    assertTrue(skipList.compareHeaders(a, b) > 0);
  }

  @Test
  void testCompareHeadersPrefixShorter() {
    Header a = createHeader("abc", 1);
    Header b = createHeader("abcd", 1);

    assertTrue(skipList.compareHeaders(a, b) < 0);
  }

  @Test
  void testLargeNumberOfInserts() {
    for (int i = 0; i < 100000; i++) {
      skipList.insert(createHeader("key" + String.format("%05d", i), i));
    }

    for (int i = 0; i < 100000; i++) {
      String key = "key" + String.format("%05d", i);
      Header result = skipList.get(createHeader(key, i));
      assertNotNull(result, "Key " + key + " should exist");
      assertEquals(i, result.SN());
    }
  }

  @Test
  void testEmptySkipListGet() {
    Header searchHeader = createHeader("anykey", 1);
    Header result = skipList.get(searchHeader);

    assertNull(result);
  }

  @Test
  void testEmptyKey() {
    Header header = createHeader("", 1);
    skipList.insert(header);

    Header result = skipList.get(createHeader("", 1));

    assertNotNull(result);
    assertEquals(1, result.SN());
  }

  @Test
  void testSingleCharacterKeys() {
    skipList.insert(createHeader("a", 1));
    skipList.insert(createHeader("b", 2));
    skipList.insert(createHeader("c", 3));

    assertEquals(1, skipList.get(createHeader("a", 1)).SN());
    assertEquals(2, skipList.get(createHeader("b", 2)).SN());
    assertEquals(3, skipList.get(createHeader("c", 3)).SN());
  }

  @Test
  void testVeryLongKey() {
    String longKey = "k".repeat(500);
    Header header = createHeader(longKey, 1);
    skipList.insert(header);

    Header result = skipList.get(createHeader(longKey, 1));

    assertNotNull(result);
    assertEquals(1, result.SN());
  }

  @Test
  void testKeysWithSamePrefix() {
    skipList.insert(createHeader("prefix", 1));
    skipList.insert(createHeader("prefix1", 2));
    skipList.insert(createHeader("prefix12", 3));
    skipList.insert(createHeader("prefix123", 4));

    assertEquals(1, skipList.get(createHeader("prefix", 1)).SN());
    assertEquals(2, skipList.get(createHeader("prefix1", 2)).SN());
    assertEquals(3, skipList.get(createHeader("prefix12", 3)).SN());
    assertEquals(4, skipList.get(createHeader("prefix123", 4)).SN());
  }

  @Test
  void testBinaryKeyData() {
    byte[] key1 = new byte[]{(byte) 0x00, (byte) 0x01};
    byte[] key2 = new byte[]{(byte) 0xFF, (byte) 0xFE};

    Header header1 = new Header(key1.length, MemorySegment.ofArray(key1), 1, TYPE_PUT);
    Header header2 = new Header(key2.length, MemorySegment.ofArray(key2), 2, TYPE_PUT);

    skipList.insert(header1);
    skipList.insert(header2);

    assertNotNull(skipList.get(new Header(key1.length, MemorySegment.ofArray(key1), 1, TYPE_PUT)));
    assertNotNull(skipList.get(new Header(key2.length, MemorySegment.ofArray(key2), 2, TYPE_PUT)));
  }

  @Test
  void testNegativeSN() {
    Header header = createHeader("key", -1);
    skipList.insert(header);

    Header result = skipList.get(createHeader("key", -1));
    assertNotNull(result);
    assertEquals(-1, result.SN());
  }

  @Test
  void testMaxIntSN() {
    Header header = createHeader("key", Integer.MAX_VALUE);
    skipList.insert(header);

    Header result = skipList.get(createHeader("key", Integer.MAX_VALUE));
    assertNotNull(result);
    assertEquals(Integer.MAX_VALUE, result.SN());
  }

  @Test
  void testMaxLongSN() {
    Header header = createHeader("key", Long.MAX_VALUE);
    skipList.insert(header);

    Header result = skipList.get(createHeader("key", Long.MAX_VALUE));
    assertNotNull(result);
    assertEquals(Long.MAX_VALUE, result.SN());
  }

  @Test
  void testMinLongSN() {
    Header header = createHeader("key", Long.MIN_VALUE);
    skipList.insert(header);

    Header result = skipList.get(createHeader("key", Long.MIN_VALUE));
    assertNotNull(result);
    assertEquals(Long.MIN_VALUE, result.SN());
  }

  @Test
  void testLongSNBeyondIntRange() {
    long largeSN = (long) Integer.MAX_VALUE + 100L;
    Header header = createHeader("key", largeSN);
    skipList.insert(header);

    Header result = skipList.get(createHeader("key", largeSN));
    assertNotNull(result);
    assertEquals(largeSN, result.SN());
  }

  @Test
  void testMultipleLongSNsForSameKey() {
    long sn1 = 1L;
    long sn2 = (long) Integer.MAX_VALUE + 1L;
    long sn3 = Long.MAX_VALUE;

    skipList.insert(createHeader("key", sn1));
    skipList.insert(createHeader("key", sn2));
    skipList.insert(createHeader("key", sn3));

    assertEquals(sn1, skipList.get(createHeader("key", sn1)).SN());
    assertEquals(sn2, skipList.get(createHeader("key", sn2)).SN());
    assertEquals(sn3, skipList.get(createHeader("key", sn3)).SN());
  }

  @Test
  void testLongSNDescendingOrderWithinSameKey() {
    long sn1 = 1_000_000_000_000L;
    long sn2 = 2_000_000_000_000L;
    long sn3 = 3_000_000_000_000L;

    skipList.insert(createHeader("key", sn1));
    skipList.insert(createHeader("key", sn3));
    skipList.insert(createHeader("key", sn2));

    // SN ordering is descending within same key (higher SN comes first)
    Header r3 = skipList.get(createHeader("key", sn3));
    Header r2 = skipList.get(createHeader("key", sn2));
    Header r1 = skipList.get(createHeader("key", sn1));

    assertNotNull(r3);
    assertEquals(sn3, r3.SN());
    assertNotNull(r2);
    assertEquals(sn2, r2.SN());
    assertNotNull(r1);
    assertEquals(sn1, r1.SN());
  }

  @Test
  void testCompareHeadersLongSNDescending() {
    long highSN = 5_000_000_000L;
    long lowSN = 1_000_000_000L;

    Header a = createHeader("test", highSN);
    Header b = createHeader("test", lowSN);

    // Higher SN should come first (compare returns negative)
    assertTrue(skipList.compareHeaders(a, b) < 0);
  }

  @Test
  void testCompareHeadersLongSNAscending() {
    long lowSN = 1_000_000_000L;
    long highSN = 5_000_000_000L;

    Header a = createHeader("test", lowSN);
    Header b = createHeader("test", highSN);

    // Lower SN should come after (compare returns positive)
    assertTrue(skipList.compareHeaders(a, b) > 0);
  }

  @Test
  void testInsertionOrder() {
    String[] keys = {"delta", "alpha", "gamma", "beta"};
    for (int i = 0; i < keys.length; i++) {
      skipList.insert(createHeader(keys[i], i));
    }

    for (int i = 0; i < keys.length; i++) {
      assertNotNull(skipList.get(createHeader(keys[i], i)));
    }
  }

  @Test
  void testKeyWithNullBytes() {
    byte[] key = new byte[]{'a', 0x00, 'b', 0x00, 'c'};
    Header header = new Header(key.length, MemorySegment.ofArray(key), 1, TYPE_PUT);
    skipList.insert(header);

    Header result = skipList.get(new Header(key.length, MemorySegment.ofArray(key), 1, TYPE_PUT));
    assertNotNull(result);
    assertEquals(1, result.SN());
  }

  @Test
  void testVarintBoundary127() {
    String key127 = "x".repeat(127);
    skipList.insert(createHeader(key127, 1));

    Header result = skipList.get(createHeader(key127, 1));
    assertNotNull(result);
  }

  @Test
  void testVarintBoundary128() {
    String key128 = "x".repeat(128);
    skipList.insert(createHeader(key128, 1));

    Header result = skipList.get(createHeader(key128, 1));
    assertNotNull(result);
  }

  @Test
  void testConcurrentReads() throws InterruptedException {
    for (int i = 0; i < 100; i++) {
      skipList.insert(createHeader("key" + i, i));
    }

    int threadCount = 4;
    Thread[] threads = new Thread[threadCount];
    AtomicInteger failures = new AtomicInteger(0);

    for (int t = 0; t < threadCount; t++) {
      threads[t] = new Thread(() -> {
        for (int i = 0; i < 100; i++) {
          Header result = skipList.get(createHeader("key" + i, i));
          if (result == null || result.SN() != i) {
            failures.incrementAndGet();
          }
        }
      });
    }

    for (Thread thread : threads) {
      thread.start();
    }
    for (Thread thread : threads) {
      thread.join();
    }

    assertEquals(0, failures.get(), "No read failures should occur");
  }

  @Test
  void testArenaMemoryUsage() {
    int initialSize = arena.getArenaSize();
    skipList.insert(createHeader("testkey", 1));
    int afterInsertSize = arena.getArenaSize();

    assertTrue(afterInsertSize > initialSize, "Arena size should increase after insert");
  }

  @Test
  void testMultipleVersionsOfSameKey() {
    skipList.insert(createHeader("user:1", 100));
    skipList.insert(createHeader("user:1", 200));
    skipList.insert(createHeader("user:1", 300));

    assertNotNull(skipList.get(createHeader("user:1", 300)));
    assertNotNull(skipList.get(createHeader("user:1", 200)));
    assertNotNull(skipList.get(createHeader("user:1", 100)));
  }

  @Test
  void testSNOrderingWithMixedKeys() {
    skipList.insert(createHeader("a", 1));
    skipList.insert(createHeader("a", 3));
    skipList.insert(createHeader("b", 2));
    skipList.insert(createHeader("a", 2));

    assertEquals(3, skipList.get(createHeader("a", 3)).SN());
    assertEquals(2, skipList.get(createHeader("a", 2)).SN());
    assertEquals(1, skipList.get(createHeader("a", 1)).SN());
    assertEquals(2, skipList.get(createHeader("b", 2)).SN());
  }

  @Test
  void testPrintOrderedKeysAndSNs() {
    skipList.insert(createHeader("apple", 1));
    skipList.insert(createHeader("apple", 3));
    skipList.insert(createHeader("apple", 2));
    skipList.insert(createHeader("banana", 5));
    skipList.insert(createHeader("banana", 2));
    skipList.insert(createHeader("cherry", 1));

    System.out.println("=== SkipList Ordering (Expected: keys ASC, SNs DESC within same key) ===");
    System.out.println("Expected order:");
    System.out.println("  apple:3, apple:2, apple:1, banana:5, banana:2, cherry:1");
    System.out.println("\nActual order:");
    skipList.forEach(header -> {
      byte[] keyBytes = header.key().toArray(ValueLayout.JAVA_BYTE);
      String key = new String(keyBytes, StandardCharsets.UTF_8);
      System.out.println("  " + key + ":" + header.SN());
    });
  }

  @Test
  void testHeaderTypeIsStoredAndRetrieved() {
    Header header = createHeader("key1", 1, TYPE_PUT);
    skipList.insert(header);

    Header result = skipList.get(createHeader("key1", 1, TYPE_PUT));
    assertNotNull(result);
    assertEquals(TYPE_PUT, result.type());
  }

  @Test
  void testDeleteTypeIsStoredAndRetrieved() {
    Header header = createHeader("key1", 2, TYPE_DELETE);
    skipList.insert(header);

    Header result = skipList.get(createHeader("key1", 2, TYPE_DELETE));
    assertNotNull(result);
    assertEquals(TYPE_DELETE, result.type());
  }

  @Test
  void testPutAndDeleteSameKeySameSN() {
    skipList.insert(createHeader("key1", 1, TYPE_PUT));
    skipList.insert(createHeader("key1", 1, TYPE_DELETE));

    // Both should be retrievable if they are treated as equal by compareHeaders
    // (compareHeaders only compares key and SN, not type)
    Header result = skipList.get(createHeader("key1", 1, TYPE_PUT));
    assertNotNull(result);
    assertEquals(1, result.SN());
  }

  @Test
  void testPutFollowedByDeleteDifferentSN() {
    skipList.insert(createHeader("key1", 1, TYPE_PUT));
    skipList.insert(createHeader("key1", 2, TYPE_DELETE));

    Header putResult = skipList.get(createHeader("key1", 1, TYPE_PUT));
    Header deleteResult = skipList.get(createHeader("key1", 2, TYPE_DELETE));

    assertNotNull(putResult);
    assertEquals(TYPE_PUT, putResult.type());
    assertEquals(1, putResult.SN());

    assertNotNull(deleteResult);
    assertEquals(TYPE_DELETE, deleteResult.type());
    assertEquals(2, deleteResult.SN());
  }

  @Test
  void testMultipleTypesAcrossKeys() {
    skipList.insert(createHeader("a", 1, TYPE_PUT));
    skipList.insert(createHeader("b", 2, TYPE_DELETE));
    skipList.insert(createHeader("c", 3, TYPE_PUT));
    skipList.insert(createHeader("d", 4, TYPE_DELETE));

    assertEquals(TYPE_PUT, skipList.get(createHeader("a", 1)).type());
    assertEquals(TYPE_DELETE, skipList.get(createHeader("b", 2)).type());
    assertEquals(TYPE_PUT, skipList.get(createHeader("c", 3)).type());
    assertEquals(TYPE_DELETE, skipList.get(createHeader("d", 4)).type());
  }

  @Test
  void testTypePreservedWithMultipleVersions() {
    skipList.insert(createHeader("doc", 1, TYPE_PUT));
    skipList.insert(createHeader("doc", 2, TYPE_PUT));
    skipList.insert(createHeader("doc", 3, TYPE_DELETE));

    Header v1 = skipList.get(createHeader("doc", 1));
    Header v2 = skipList.get(createHeader("doc", 2));
    Header v3 = skipList.get(createHeader("doc", 3));

    assertNotNull(v1);
    assertEquals(TYPE_PUT, v1.type());
    assertNotNull(v2);
    assertEquals(TYPE_PUT, v2.type());
    assertNotNull(v3);
    assertEquals(TYPE_DELETE, v3.type());
  }

  @Test
  void testForEachIncludesType() {
    skipList.insert(createHeader("x", 1, TYPE_PUT));
    skipList.insert(createHeader("y", 2, TYPE_DELETE));

    AtomicInteger count = new AtomicInteger(0);
    skipList.forEach(header -> {
      assertNotNull(header);
      assertTrue(header.type() == TYPE_PUT || header.type() == TYPE_DELETE);
      count.incrementAndGet();
    });

    assertEquals(2, count.get());
  }

  @Test
  void testAllByteValuesForType() {
    for (int t = Byte.MIN_VALUE; t <= Byte.MAX_VALUE; t++) {
      arena = new ArenaImpl();
      skipList = new SkipList(arena);
      skipList.init();

      byte type = (byte) t;
      skipList.insert(createHeader("key", 1, type));

      Header result = skipList.get(createHeader("key", 1, type));
      assertNotNull(result, "Should find header with type=" + t);
      assertEquals(type, result.type(), "Type should match for value=" + t);

      arena.close();
    }
  }

  @Test
  void testTypeWithLongSN() {
    long largeSN = 9_999_999_999L;
    skipList.insert(createHeader("key1", largeSN, TYPE_PUT));
    skipList.insert(createHeader("key2", largeSN, TYPE_DELETE));

    Header putResult = skipList.get(createHeader("key1", largeSN));
    Header deleteResult = skipList.get(createHeader("key2", largeSN));

    assertNotNull(putResult);
    assertEquals(TYPE_PUT, putResult.type());
    assertEquals(largeSN, putResult.SN());

    assertNotNull(deleteResult);
    assertEquals(TYPE_DELETE, deleteResult.type());
    assertEquals(largeSN, deleteResult.SN());
  }

  @Test
  void testDeleteThenPutDifferentSN() {
    skipList.insert(createHeader("key1", 1, TYPE_DELETE));
    skipList.insert(createHeader("key1", 2, TYPE_PUT));

    Header v1 = skipList.get(createHeader("key1", 1));
    Header v2 = skipList.get(createHeader("key1", 2));

    assertNotNull(v1);
    assertEquals(TYPE_DELETE, v1.type());
    assertNotNull(v2);
    assertEquals(TYPE_PUT, v2.type());
  }

  @Test
  void testForEachOrderWithTypes() {
    skipList.insert(createHeader("a", 2, TYPE_DELETE));
    skipList.insert(createHeader("a", 1, TYPE_PUT));
    skipList.insert(createHeader("b", 1, TYPE_PUT));

    List<String> order = new ArrayList<>();
    skipList.forEach(header -> {
      byte[] keyBytes = header.key().toArray(ValueLayout.JAVA_BYTE);
      String key = new String(keyBytes, StandardCharsets.UTF_8);
      order.add(key + ":" + header.SN() + ":" + header.type());
    });

    // Expected: keys ASC, SNs DESC within same key
    assertEquals(3, order.size());
    assertEquals("a:2:1", order.get(0)); // a with SN=2 (DELETE=1) first
    assertEquals("a:1:0", order.get(1)); // a with SN=1 (PUT=0) second
    assertEquals("b:1:0", order.get(2)); // b with SN=1 (PUT=0)
  }

  @Test
  void testTypePreservedAfterManyInserts() {
    for (int i = 0; i < 1000; i++) {
      byte type = (i % 2 == 0) ? TYPE_PUT : TYPE_DELETE;
      skipList.insert(createHeader("key" + String.format("%04d", i), i, type));
    }

    for (int i = 0; i < 1000; i++) {
      byte expectedType = (i % 2 == 0) ? TYPE_PUT : TYPE_DELETE;
      String key = "key" + String.format("%04d", i);
      Header result = skipList.get(createHeader(key, i));
      assertNotNull(result, "Key " + key + " should exist");
      assertEquals(expectedType, result.type(), "Type mismatch for key " + key);
    }
  }

  @Test
  void testLongSNWithNegativeValues() {
    long negativeSN = -5_000_000_000L;
    skipList.insert(createHeader("key", negativeSN));

    Header result = skipList.get(createHeader("key", negativeSN));
    assertNotNull(result);
    assertEquals(negativeSN, result.SN());
  }

  @Test
  void testMixedLongSNsAcrossKeys() {
    skipList.insert(createHeader("alpha", Long.MIN_VALUE));
    skipList.insert(createHeader("beta", 0L));
    skipList.insert(createHeader("gamma", Long.MAX_VALUE));

    assertEquals(Long.MIN_VALUE, skipList.get(createHeader("alpha", Long.MIN_VALUE)).SN());
    assertEquals(0L, skipList.get(createHeader("beta", 0L)).SN());
    assertEquals(Long.MAX_VALUE, skipList.get(createHeader("gamma", Long.MAX_VALUE)).SN());
  }

  @Test
  void testConcurrentReadsWithTypes() throws InterruptedException {
    for (int i = 0; i < 100; i++) {
      byte type = (i % 2 == 0) ? TYPE_PUT : TYPE_DELETE;
      skipList.insert(createHeader("key" + i, i, type));
    }

    int threadCount = 4;
    Thread[] threads = new Thread[threadCount];
    AtomicInteger failures = new AtomicInteger(0);

    for (int t = 0; t < threadCount; t++) {
      threads[t] = new Thread(() -> {
        for (int i = 0; i < 100; i++) {
          byte expectedType = (i % 2 == 0) ? TYPE_PUT : TYPE_DELETE;
          Header result = skipList.get(createHeader("key" + i, i));
          if (result == null || result.SN() != i || result.type() != expectedType) {
            failures.incrementAndGet();
          }
        }
      });
    }

    for (Thread thread : threads) {
      thread.start();
    }
    for (Thread thread : threads) {
      thread.join();
    }

    assertEquals(0, failures.get(), "No read failures should occur with types");
  }

  @Test
  void testPrintOrderedKeysWithTypes() {
    skipList.insert(createHeader("apple", 2, TYPE_DELETE));
    skipList.insert(createHeader("apple", 1, TYPE_PUT));
    skipList.insert(createHeader("banana", 3, TYPE_PUT));
    skipList.insert(createHeader("banana", 1, TYPE_DELETE));

    System.out.println("=== SkipList Ordering with Types ===");
    skipList.forEach(header -> {
      byte[] keyBytes = header.key().toArray(ValueLayout.JAVA_BYTE);
      String key = new String(keyBytes, StandardCharsets.UTF_8);
      String typeStr = header.type() == TYPE_PUT ? "PUT" : "DELETE";
      System.out.println("  " + key + ":SN=" + header.SN() + ":" + typeStr);
    });
  }
}
