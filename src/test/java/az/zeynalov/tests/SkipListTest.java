package az.zeynalov.tests;

import az.zeynalov.memtable.ArenaImpl;
import az.zeynalov.memtable.Header;
import az.zeynalov.memtable.SkipList;
import java.lang.foreign.ValueLayout;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class SkipListTest {

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

  private Header createHeader(String key, int sn) {
    byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
    MemorySegment keySegment = MemorySegment.ofArray(keyBytes);
    return new Header(keyBytes.length, keySegment, sn);
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

    Header header1 = new Header(key1.length, MemorySegment.ofArray(key1), 1);
    Header header2 = new Header(key2.length, MemorySegment.ofArray(key2), 2);

    skipList.insert(header1);
    skipList.insert(header2);

    assertNotNull(skipList.get(new Header(key1.length, MemorySegment.ofArray(key1), 1)));
    assertNotNull(skipList.get(new Header(key2.length, MemorySegment.ofArray(key2), 2)));
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
    Header header = new Header(key.length, MemorySegment.ofArray(key), 1);
    skipList.insert(header);

    Header result = skipList.get(new Header(key.length, MemorySegment.ofArray(key), 1));
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

}
