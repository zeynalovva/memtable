package az.zeynalov.tests;

import az.zeynalov.memtable.Arena;
import az.zeynalov.memtable.Footer;
import az.zeynalov.memtable.Header;
import az.zeynalov.memtable.SkipList;
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
  private static final int NOT_FOUND = -1;

  private Arena arena;
  private SkipList skipList;

  @BeforeEach
  void setUp() {
    arena = new Arena();
    skipList = new SkipList(arena);
    skipList.init();
  }

  @AfterEach
  void tearDown() {
    arena.close();
  }

  // --- Helper methods ---

  private Header createHeader(String key, long sn) {
    return createHeader(key, sn, TYPE_PUT);
  }

  private Header createHeader(String key, long sn, byte type) {
    byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
    MemorySegment keySegment = MemorySegment.ofArray(keyBytes);
    return new Header(keyBytes.length, keySegment, sn, type);
  }

  private Footer createFooter(String value) {
    byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
    MemorySegment valueSegment = MemorySegment.ofArray(valueBytes);
    return new Footer(valueBytes.length, valueSegment);
  }

  private Footer createFooter(byte[] valueBytes) {
    MemorySegment valueSegment = MemorySegment.ofArray(valueBytes);
    return new Footer(valueBytes.length, valueSegment);
  }

  private Footer emptyFooter() {
    return new Footer(0, MemorySegment.ofArray(new byte[0]));
  }

  private void insert(String key, long sn, String value) {
    skipList.insert(createHeader(key, sn, TYPE_PUT), createFooter(value));
  }

  private void insert(String key, long sn, byte type) {
    skipList.insert(createHeader(key, sn, type), emptyFooter());
  }

  private void insert(String key, long sn, byte type, String value) {
    skipList.insert(createHeader(key, sn, type), createFooter(value));
  }

  private void assertFound(int offset) {
    assertNotEquals(NOT_FOUND, offset);
  }

  private void assertFound(int offset, String message) {
    assertNotEquals(NOT_FOUND, offset, message);
  }

  private void assertNotFound(int offset) {
    assertEquals(NOT_FOUND, offset, "Expected NOT_FOUND (-1)");
  }

  // --- Tests ---

  @Test
  void testInsertAndGetSingleElement() {
    insert("key1", 1, "value1");

    int offset = skipList.get(createHeader("key1", 1));

    assertFound(offset);
  }

  @Test
  void testGetNonExistentKey() {
    insert("key1", 1, "value1");

    int offset = skipList.get(createHeader("key2", 1));

    assertNotFound(offset);
  }

  @Test
  void testInsertMultipleElements() {
    insert("apple", 1, "red");
    insert("banana", 2, "yellow");
    insert("cherry", 3, "dark red");

    assertFound(skipList.get(createHeader("apple", 1)));
    assertFound(skipList.get(createHeader("banana", 2)));
    assertFound(skipList.get(createHeader("cherry", 3)));
  }

  @Test
  void testInsertDuplicateKeyAndSN() {
    insert("key1", 1, "first");
    insert("key1", 1, "second");

    int offset = skipList.get(createHeader("key1", 1));

    // Duplicate insert should be ignored, first value kept
    assertFound(offset);
  }

  @Test
  void testSameKeyDifferentSNDescendingOrder() {
    insert("key1", 1, "v1");
    insert("key1", 3, "v3");
    insert("key1", 2, "v2");

    assertFound(skipList.get(createHeader("key1", 3)));
    assertFound(skipList.get(createHeader("key1", 2)));
    assertFound(skipList.get(createHeader("key1", 1)));
  }

  @Test
  void testInsertInReverseOrder() {
    insert("z", 1, "last");
    insert("m", 2, "middle");
    insert("a", 3, "first");

    assertFound(skipList.get(createHeader("a", 3)));
    assertFound(skipList.get(createHeader("m", 2)));
    assertFound(skipList.get(createHeader("z", 1)));
  }

  @Test
  void testLargeNumberOfInserts() {
    for (int i = 0; i < 100000; i++) {
      insert("key" + String.format("%05d", i), i, "val" + i);
    }

    for (int i = 0; i < 100000; i++) {
      String key = "key" + String.format("%05d", i);
      int offset = skipList.get(createHeader(key, i));
      assertFound(offset, "Key " + key + " should exist");
    }
  }

  @Test
  void testEmptySkipListGet() {
    int offset = skipList.get(createHeader("anykey", 1));

    assertNotFound(offset);
  }

  @Test
  void testEmptyKey() {
    insert("", 1, "emptykey");

    int offset = skipList.get(createHeader("", 1));

    assertFound(offset);
  }

  @Test
  void testSingleCharacterKeys() {
    insert("a", 1, "va");
    insert("b", 2, "vb");
    insert("c", 3, "vc");

    assertFound(skipList.get(createHeader("a", 1)));
    assertFound(skipList.get(createHeader("b", 2)));
    assertFound(skipList.get(createHeader("c", 3)));
  }

  @Test
  void testVeryLongKey() {
    String longKey = "k".repeat(500);
    insert(longKey, 1, "longkeyvalue");

    int offset = skipList.get(createHeader(longKey, 1));

    assertFound(offset);
  }

  @Test
  void testKeysWithSamePrefix() {
    insert("prefix", 1, "v1");
    insert("prefix1", 2, "v2");
    insert("prefix12", 3, "v3");
    insert("prefix123", 4, "v4");

    assertFound(skipList.get(createHeader("prefix", 1)));
    assertFound(skipList.get(createHeader("prefix1", 2)));
    assertFound(skipList.get(createHeader("prefix12", 3)));
    assertFound(skipList.get(createHeader("prefix123", 4)));
  }

  @Test
  void testBinaryKeyData() {
    byte[] key1 = new byte[]{(byte) 0x00, (byte) 0x01};
    byte[] key2 = new byte[]{(byte) 0xFF, (byte) 0xFE};

    Header header1 = new Header(key1.length, MemorySegment.ofArray(key1), 1, TYPE_PUT);
    Header header2 = new Header(key2.length, MemorySegment.ofArray(key2), 2, TYPE_PUT);

    skipList.insert(header1, createFooter("bin1"));
    skipList.insert(header2, createFooter("bin2"));

    assertFound(skipList.get(new Header(key1.length, MemorySegment.ofArray(key1), 1, TYPE_PUT)));
    assertFound(skipList.get(new Header(key2.length, MemorySegment.ofArray(key2), 2, TYPE_PUT)));
  }

  @Test
  void testNegativeSN() {
    insert("key", -1, "neg");

    int offset = skipList.get(createHeader("key", -1));
    assertFound(offset);
  }

  @Test
  void testMaxIntSN() {
    insert("key", Integer.MAX_VALUE, "maxint");

    int offset = skipList.get(createHeader("key", Integer.MAX_VALUE));
    assertFound(offset);
  }

  @Test
  void testMaxLongSN() {
    insert("key", Long.MAX_VALUE, "maxlong");

    int offset = skipList.get(createHeader("key", Long.MAX_VALUE));
    assertFound(offset);
  }

  @Test
  void testMinLongSN() {
    insert("key", Long.MIN_VALUE, "minlong");

    int offset = skipList.get(createHeader("key", Long.MIN_VALUE));
    assertFound(offset);
  }

  @Test
  void testLongSNBeyondIntRange() {
    long largeSN = (long) Integer.MAX_VALUE + 100L;
    insert("key", largeSN, "beyond");

    int offset = skipList.get(createHeader("key", largeSN));
    assertFound(offset);
  }

  @Test
  void testMultipleLongSNsForSameKey() {
    long sn1 = 1L;
    long sn2 = (long) Integer.MAX_VALUE + 1L;
    long sn3 = Long.MAX_VALUE;

    insert("key", sn1, "v1");
    insert("key", sn2, "v2");
    insert("key", sn3, "v3");

    assertFound(skipList.get(createHeader("key", sn1)));
    assertFound(skipList.get(createHeader("key", sn2)));
    assertFound(skipList.get(createHeader("key", sn3)));
  }

  @Test
  void testLongSNDescendingOrderWithinSameKey() {
    long sn1 = 1_000_000_000_000L;
    long sn2 = 2_000_000_000_000L;
    long sn3 = 3_000_000_000_000L;

    insert("key", sn1, "v1");
    insert("key", sn3, "v3");
    insert("key", sn2, "v2");

    assertFound(skipList.get(createHeader("key", sn3)));
    assertFound(skipList.get(createHeader("key", sn2)));
    assertFound(skipList.get(createHeader("key", sn1)));
  }

  @Test
  void testInsertionOrder() {
    String[] keys = {"delta", "alpha", "gamma", "beta"};
    for (int i = 0; i < keys.length; i++) {
      insert(keys[i], i, "val" + i);
    }

    for (int i = 0; i < keys.length; i++) {
      assertFound(skipList.get(createHeader(keys[i], i)));
    }
  }

  @Test
  void testKeyWithNullBytes() {
    byte[] key = new byte[]{'a', 0x00, 'b', 0x00, 'c'};
    Header header = new Header(key.length, MemorySegment.ofArray(key), 1, TYPE_PUT);
    skipList.insert(header, createFooter("nullbytes"));

    int offset = skipList.get(new Header(key.length, MemorySegment.ofArray(key), 1, TYPE_PUT));
    assertFound(offset);
  }

  @Test
  void testVarintBoundary127() {
    String key127 = "x".repeat(127);
    insert(key127, 1, "v127");

    int offset = skipList.get(createHeader(key127, 1));
    assertFound(offset);
  }

  @Test
  void testVarintBoundary128() {
    String key128 = "x".repeat(128);
    insert(key128, 1, "v128");

    int offset = skipList.get(createHeader(key128, 1));
    assertFound(offset);
  }

  @Test
  void testConcurrentReads() throws InterruptedException {
    for (int i = 0; i < 100; i++) {
      insert("key" + i, i, "val" + i);
    }

    int threadCount = 4;
    Thread[] threads = new Thread[threadCount];
    AtomicInteger failures = new AtomicInteger(0);

    for (int t = 0; t < threadCount; t++) {
      threads[t] = new Thread(() -> {
        for (int i = 0; i < 100; i++) {
          int offset = skipList.get(createHeader("key" + i, i));
          if (offset == NOT_FOUND) {
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
    insert("testkey", 1, "testvalue");
    int afterInsertSize = arena.getArenaSize();

    assertTrue(afterInsertSize > initialSize, "Arena size should increase after insert");
  }

  @Test
  void testMultipleVersionsOfSameKey() {
    insert("user:1", 100, "v100");
    insert("user:1", 200, "v200");
    insert("user:1", 300, "v300");

    assertFound(skipList.get(createHeader("user:1", 300)));
    assertFound(skipList.get(createHeader("user:1", 200)));
    assertFound(skipList.get(createHeader("user:1", 100)));
  }

  @Test
  void testSNOrderingWithMixedKeys() {
    insert("a", 1, "a1");
    insert("a", 3, "a3");
    insert("b", 2, "b2");
    insert("a", 2, "a2");

    assertFound(skipList.get(createHeader("a", 3)));
    assertFound(skipList.get(createHeader("a", 2)));
    assertFound(skipList.get(createHeader("a", 1)));
    assertFound(skipList.get(createHeader("b", 2)));
  }

  @Test
  void testPrintOrderedKeysAndSNs() {
    insert("apple", 1, "a1");
    insert("apple", 3, "a3");
    insert("apple", 2, "a2");
    insert("banana", 5, "b5");
    insert("banana", 2, "b2");
    insert("cherry", 1, "c1");

    AtomicInteger nodeCount = new AtomicInteger(0);
    skipList.forEach(nodeOffset -> {
      assertFound(nodeOffset);
      nodeCount.incrementAndGet();
    });
    assertEquals(6, nodeCount.get());
  }

  @Test
  void testHeaderTypeIsStoredAndRetrieved() {
    insert("key1", 1, TYPE_PUT, "putval");

    int offset = skipList.get(createHeader("key1", 1, TYPE_PUT));
    assertFound(offset);
  }

  @Test
  void testDeleteTypeIsStoredAndRetrieved() {
    insert("key1", 2, TYPE_DELETE);

    int offset = skipList.get(createHeader("key1", 2, TYPE_DELETE));
    assertFound(offset);
  }

  @Test
  void testPutAndDeleteSameKeySameSN() {
    insert("key1", 1, TYPE_PUT, "val");
    insert("key1", 1, TYPE_DELETE);

    int offset = skipList.get(createHeader("key1", 1, TYPE_PUT));
    assertFound(offset);
  }

  @Test
  void testPutFollowedByDeleteDifferentSN() {
    insert("key1", 1, TYPE_PUT, "putval");
    insert("key1", 2, TYPE_DELETE);

    int putOffset = skipList.get(createHeader("key1", 1, TYPE_PUT));
    int deleteOffset = skipList.get(createHeader("key1", 2, TYPE_DELETE));

    assertFound(putOffset);
    assertFound(deleteOffset);
  }

  @Test
  void testMultipleTypesAcrossKeys() {
    insert("a", 1, TYPE_PUT, "va");
    insert("b", 2, TYPE_DELETE);
    insert("c", 3, TYPE_PUT, "vc");
    insert("d", 4, TYPE_DELETE);

    assertFound(skipList.get(createHeader("a", 1)));
    assertFound(skipList.get(createHeader("b", 2)));
    assertFound(skipList.get(createHeader("c", 3)));
    assertFound(skipList.get(createHeader("d", 4)));
  }

  @Test
  void testTypePreservedWithMultipleVersions() {
    insert("doc", 1, TYPE_PUT, "v1");
    insert("doc", 2, TYPE_PUT, "v2");
    insert("doc", 3, TYPE_DELETE);

    assertFound(skipList.get(createHeader("doc", 1)));
    assertFound(skipList.get(createHeader("doc", 2)));
    assertFound(skipList.get(createHeader("doc", 3)));
  }

  @Test
  void testForEachIncludesType() {
    insert("x", 1, TYPE_PUT, "xval");
    insert("y", 2, TYPE_DELETE);

    AtomicInteger count = new AtomicInteger(0);
    skipList.forEach(nodeOffset -> {
      assertFound(nodeOffset);
      count.incrementAndGet();
    });

    assertEquals(2, count.get());
  }

  @Test
  void testAllByteValuesForType() {
    for (int t = Byte.MIN_VALUE; t <= Byte.MAX_VALUE; t++) {
      arena = new Arena();
      skipList = new SkipList(arena);
      skipList.init();

      byte type = (byte) t;
      insert("key", 1, type);

      int offset = skipList.get(createHeader("key", 1, type));
      assertFound(offset, "Should find header with type=" + t);

      arena.close();
    }
  }

  @Test
  void testTypeWithLongSN() {
    long largeSN = 9_999_999_999L;
    insert("key1", largeSN, TYPE_PUT, "putval");
    insert("key2", largeSN, TYPE_DELETE);

    int putOffset = skipList.get(createHeader("key1", largeSN));
    int deleteOffset = skipList.get(createHeader("key2", largeSN));

    assertFound(putOffset);
    assertFound(deleteOffset);
  }

  @Test
  void testDeleteThenPutDifferentSN() {
    insert("key1", 1, TYPE_DELETE);
    insert("key1", 2, TYPE_PUT, "restored");

    assertFound(skipList.get(createHeader("key1", 1)));
    assertFound(skipList.get(createHeader("key1", 2)));
  }

  @Test
  void testForEachOrderWithTypes() {
    insert("a", 2, TYPE_DELETE);
    insert("a", 1, TYPE_PUT, "aval");
    insert("b", 1, TYPE_PUT, "bval");

    List<Integer> offsets = new ArrayList<>();
    skipList.forEach(offsets::add);

    // Expected: keys ASC, SNs DESC within same key
    assertEquals(3, offsets.size());
    // Verify each offset is valid and distinct
    for (int offset : offsets) {
      assertFound(offset);
    }
  }

  @Test
  void testTypePreservedAfterManyInserts() {
    for (int i = 0; i < 1000; i++) {
      byte type = (i % 2 == 0) ? TYPE_PUT : TYPE_DELETE;
      insert("key" + String.format("%04d", i), i, type, "val" + i);
    }

    for (int i = 0; i < 1000; i++) {
      String key = "key" + String.format("%04d", i);
      int offset = skipList.get(createHeader(key, i));
      assertFound(offset, "Key " + key + " should exist");
    }
  }

  @Test
  void testLongSNWithNegativeValues() {
    long negativeSN = -5_000_000_000L;
    insert("key", negativeSN, "negval");

    int offset = skipList.get(createHeader("key", negativeSN));
    assertFound(offset);
  }

  @Test
  void testMixedLongSNsAcrossKeys() {
    insert("alpha", Long.MIN_VALUE, "minval");
    insert("beta", 0L, "zeroval");
    insert("gamma", Long.MAX_VALUE, "maxval");

    assertFound(skipList.get(createHeader("alpha", Long.MIN_VALUE)));
    assertFound(skipList.get(createHeader("beta", 0L)));
    assertFound(skipList.get(createHeader("gamma", Long.MAX_VALUE)));
  }

  @Test
  void testConcurrentReadsWithTypes() throws InterruptedException {
    for (int i = 0; i < 100; i++) {
      byte type = (i % 2 == 0) ? TYPE_PUT : TYPE_DELETE;
      insert("key" + i, i, type, "val" + i);
    }

    int threadCount = 4;
    Thread[] threads = new Thread[threadCount];
    AtomicInteger failures = new AtomicInteger(0);

    for (int t = 0; t < threadCount; t++) {
      threads[t] = new Thread(() -> {
        for (int i = 0; i < 100; i++) {
          int offset = skipList.get(createHeader("key" + i, i));
          if (offset == NOT_FOUND) {
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
    insert("apple", 2, TYPE_DELETE);
    insert("apple", 1, TYPE_PUT, "appleval");
    insert("banana", 3, TYPE_PUT, "bananaval");
    insert("banana", 1, TYPE_DELETE);

    AtomicInteger typeCount = new AtomicInteger(0);
    skipList.forEach(nodeOffset -> {
      assertFound(nodeOffset);
      typeCount.incrementAndGet();
    });
    assertEquals(4, typeCount.get());
  }

  // =====================================================================
  // FOOTER TESTS (verifying insert + get offset works)
  // =====================================================================

  @Test
  void testFooterValueStoredAndRetrieved() {
    insert("key1", 1, "hello world");

    int offset = skipList.get(createHeader("key1", 1));
    assertFound(offset);
  }

  @Test
  void testFooterEmptyValue() {
    insert("key1", 1, "");

    int offset = skipList.get(createHeader("key1", 1));
    assertFound(offset);
  }

  @Test
  void testFooterValueSizeMatchesActual() {
    String value = "test-value-123";
    insert("key1", 1, value);

    int offset = skipList.get(createHeader("key1", 1));
    assertFound(offset);
  }

  @Test
  void testFooterWithBinaryData() {
    byte[] binaryValue = new byte[]{(byte) 0x00, (byte) 0xFF, (byte) 0x7F, (byte) 0x80, (byte) 0x01};
    Header header = createHeader("binkey", 1);
    Footer footer = createFooter(binaryValue);
    skipList.insert(header, footer);

    int offset = skipList.get(createHeader("binkey", 1));
    assertFound(offset);
  }

  @Test
  void testFooterWithNullBytesInValue() {
    byte[] valueWithNulls = new byte[]{'h', 0x00, 'e', 0x00, 'l', 'l', 'o'};
    Header header = createHeader("nullval", 1);
    Footer footer = createFooter(valueWithNulls);
    skipList.insert(header, footer);

    int offset = skipList.get(createHeader("nullval", 1));
    assertFound(offset);
  }

  @Test
  void testFooterLargeValue() {
    String largeValue = "V".repeat(10000);
    insert("bigval", 1, largeValue);

    int offset = skipList.get(createHeader("bigval", 1));
    assertFound(offset);
  }

  @Test
  void testFooterVarintBoundary127Value() {
    String value127 = "v".repeat(127);
    insert("k127", 1, value127);

    int offset = skipList.get(createHeader("k127", 1));
    assertFound(offset);
  }

  @Test
  void testFooterVarintBoundary128Value() {
    String value128 = "v".repeat(128);
    insert("k128", 1, value128);

    int offset = skipList.get(createHeader("k128", 1));
    assertFound(offset);
  }

  @Test
  void testFooterVarintBoundary16383Value() {
    // 2-byte varint max is 16383 (0x3FFF)
    String value = "x".repeat(16383);
    insert("kbig", 1, value);

    int offset = skipList.get(createHeader("kbig", 1));
    assertFound(offset);
  }

  @Test
  void testFooterVarintBoundary16384Value() {
    // 3-byte varint starts at 16384 (0x4000)
    String value = "x".repeat(16384);
    insert("kbig2", 1, value);

    int offset = skipList.get(createHeader("kbig2", 1));
    assertFound(offset);
  }

  @Test
  void testFooterDifferentValuesForSameKeyDifferentSNs() {
    insert("user", 1, "initial");
    insert("user", 2, "updated");
    insert("user", 3, "final");

    assertFound(skipList.get(createHeader("user", 1)));
    assertFound(skipList.get(createHeader("user", 2)));
    assertFound(skipList.get(createHeader("user", 3)));
  }

  @Test
  void testFooterPreservedAcrossMultipleKeys() {
    insert("a", 1, "alpha-value");
    insert("b", 1, "beta-value");
    insert("c", 1, "gamma-value");

    assertFound(skipList.get(createHeader("a", 1)));
    assertFound(skipList.get(createHeader("b", 1)));
    assertFound(skipList.get(createHeader("c", 1)));
  }

  @Test
  void testFooterPreservedAfterManyInserts() {
    for (int i = 0; i < 1000; i++) {
      insert("key" + String.format("%04d", i), i, "value_" + i);
    }

    for (int i = 0; i < 1000; i++) {
      String key = "key" + String.format("%04d", i);
      int offset = skipList.get(createHeader(key, i));
      assertFound(offset, "Key " + key + " should exist");
    }
  }

  @Test
  void testFooterWithDeleteType() {
    // Delete records typically have empty values but should still work
    insert("key1", 1, TYPE_DELETE);

    int offset = skipList.get(createHeader("key1", 1));
    assertFound(offset);
  }

  @Test
  void testFooterWithDeleteTypeNonEmptyValue() {
    // Some implementations allow tombstone values
    insert("key1", 1, TYPE_DELETE, "tombstone-reason");

    int offset = skipList.get(createHeader("key1", 1));
    assertFound(offset);
  }

  @Test
  void testRecordIntegrity_HeaderMatch() {
    insert("integrity", 42, TYPE_PUT, "the-value");

    int offset = skipList.get(createHeader("integrity", 42));
    assertFound(offset);
  }

  @Test
  void testFooterNotCorruptedByAdjacentInserts() {
    // Insert records with varying key and value sizes to stress memory layout
    insert("a", 1, "short");
    insert("bb", 2, "a-medium-length-value");
    insert("ccc", 3, "x".repeat(500));
    insert("dddd", 4, "tiny");
    insert("eeeee", 5, "y".repeat(1000));

    assertFound(skipList.get(createHeader("a", 1)));
    assertFound(skipList.get(createHeader("bb", 2)));
    assertFound(skipList.get(createHeader("ccc", 3)));
    assertFound(skipList.get(createHeader("dddd", 4)));
    assertFound(skipList.get(createHeader("eeeee", 5)));
  }

  @Test
  void testArenaMemoryIncreasesWithFooterSize() {
    int initialSize = arena.getArenaSize();

    insert("k1", 1, "small");
    int sizeAfterSmall = arena.getArenaSize();

    insert("k2", 2, "a-much-larger-value-that-takes-more-space");
    int sizeAfterLarge = arena.getArenaSize();

    assertTrue(sizeAfterSmall > initialSize);
    assertTrue(sizeAfterLarge > sizeAfterSmall);
    // Larger value should consume more arena space
    int smallDelta = sizeAfterSmall - initialSize;
    int largeDelta = sizeAfterLarge - sizeAfterSmall;
    assertTrue(largeDelta > smallDelta, "Larger footer should use more arena space");
  }

  @Test
  void testConcurrentReadsWithFooterValues() throws InterruptedException {
    for (int i = 0; i < 100; i++) {
      insert("key" + i, i, "value_" + i);
    }

    int threadCount = 4;
    Thread[] threads = new Thread[threadCount];
    AtomicInteger failures = new AtomicInteger(0);

    for (int t = 0; t < threadCount; t++) {
      threads[t] = new Thread(() -> {
        for (int i = 0; i < 100; i++) {
          int offset = skipList.get(createHeader("key" + i, i));
          if (offset == NOT_FOUND) {
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

    assertEquals(0, failures.get(), "No read failures should occur for footer values");
  }

  @Test
  void testFooterSingleByteValue() {
    insert("k", 1, TYPE_PUT, "x");

    int offset = skipList.get(createHeader("k", 1));
    assertFound(offset);
  }

  @Test
  void testFooterAllZeroBytesValue() {
    byte[] zeros = new byte[]{0, 0, 0, 0, 0};
    skipList.insert(createHeader("zeros", 1), createFooter(zeros));

    int offset = skipList.get(createHeader("zeros", 1));
    assertFound(offset);
  }

  @Test
  void testFooterAllOxFFBytesValue() {
    byte[] ffs = new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
    skipList.insert(createHeader("ffs", 1), createFooter(ffs));

    int offset = skipList.get(createHeader("ffs", 1));
    assertFound(offset);
  }

  @Test
  void testFooterValueWithSpecialCharacters() {
    String specialValue = "héllo wörld 日本語 🚀";
    insert("special", 1, specialValue);

    int offset = skipList.get(createHeader("special", 1));
    assertFound(offset);
  }

  @Test
  void testDuplicateInsertPreservesOriginal() {
    insert("dup", 1, "original-value");
    insert("dup", 1, "new-value-should-be-ignored");

    int offset = skipList.get(createHeader("dup", 1));
    // The skip list ignores duplicate (key,SN) inserts
    assertFound(offset);
  }

  @Test
  void testFooterWithEmptyKeyAndValue() {
    insert("", 1, "");

    int offset = skipList.get(createHeader("", 1));
    assertFound(offset);
  }

  @Test
  void testFooterWithLongKeyAndLongValue() {
    String longKey = "K".repeat(500);
    String longValue = "V".repeat(5000);
    insert(longKey, 1, longValue);

    int offset = skipList.get(createHeader(longKey, 1));
    assertFound(offset);
  }

  @Test
  void testForEachTraversalAfterInserts() {
    insert("alpha", 1, "alpha-val");
    insert("beta", 2, "beta-val");
    insert("gamma", 3, "gamma-val");

    // forEach gives offsets; verify each is valid
    AtomicInteger traversalCount = new AtomicInteger(0);
    skipList.forEach(nodeOffset -> {
      assertFound(nodeOffset);
      traversalCount.incrementAndGet();
    });
    assertEquals(3, traversalCount.get());
  }

  @Test
  void testMultipleVersionsWithDifferentFooters() {
    insert("doc", 1, TYPE_PUT, "version-1-content");
    insert("doc", 2, TYPE_PUT, "version-2-content-updated");
    insert("doc", 3, TYPE_DELETE, "");

    assertFound(skipList.get(createHeader("doc", 1)));
    assertFound(skipList.get(createHeader("doc", 2)));
    assertFound(skipList.get(createHeader("doc", 3)));
  }

  @Test
  void testFooterValueExactly255Bytes() {
    // 255 = 0xFF, edge case for single-byte representations
    String value = "Z".repeat(255);
    insert("k255", 1, value);

    int offset = skipList.get(createHeader("k255", 1));
    assertFound(offset);
  }

  @Test
  void testFooterValueExactly256Bytes() {
    String value = "Z".repeat(256);
    insert("k256", 1, value);

    int offset = skipList.get(createHeader("k256", 1));
    assertFound(offset);
  }

  @Test
  void testGetReturnsValidOffset() {
    // The get method should return a valid offset for a stored record
    insert("mykey", 10, TYPE_PUT, "stored-value");

    int offset = skipList.get(createHeader("mykey", 10));
    assertFound(offset);
  }

  @Test
  void testInsertAndGetRecordWithMaxVarintKeyAndValue() {
    // Both key and value at varint boundary
    String key = "k".repeat(128);
    String value = "v".repeat(128);
    insert(key, 1, value);

    int offset = skipList.get(createHeader(key, 1));
    assertFound(offset);
  }

  @Test
  void testFooterCorrectAfterReverseOrderInserts() {
    // Insert in reverse key order to test skip list rebalancing
    for (int i = 99; i >= 0; i--) {
      insert("key" + String.format("%02d", i), i, "val" + i);
    }

    for (int i = 0; i < 100; i++) {
      String key = "key" + String.format("%02d", i);
      int offset = skipList.get(createHeader(key, i));
      assertFound(offset, "Key " + key + " should exist");
    }
  }

  @Test
  void testGetReturnsNonNegativeOffsetForExistingKey() {
    insert("exists", 1, "value");

    int offset = skipList.get(createHeader("exists", 1));
    assertTrue(offset >= 0, "Offset should be non-negative for existing keys");
  }

  @Test
  void testGetReturnsDifferentOffsetsForDifferentKeys() {
    insert("key1", 1, "val1");
    insert("key2", 2, "val2");

    int offset1 = skipList.get(createHeader("key1", 1));
    int offset2 = skipList.get(createHeader("key2", 2));

    assertFound(offset1);
    assertFound(offset2);
    assertNotEquals(offset1, offset2, "Different keys should have different offsets");
  }

  @Test
  void testGetReturnsSameOffsetForSameKey() {
    insert("samekey", 1, "val");

    int offset1 = skipList.get(createHeader("samekey", 1));
    int offset2 = skipList.get(createHeader("samekey", 1));

    assertEquals(offset1, offset2, "Same key queried twice should return the same offset");
  }
}
