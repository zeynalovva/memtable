package az.zeynalov.tests;

import az.zeynalov.memtable.ArenaImpl;
import az.zeynalov.memtable.Footer;
import az.zeynalov.memtable.Header;
import az.zeynalov.memtable.Record;
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


  private Record createRecord(String key, long sn, String value) {
    return new Record(createHeader(key, sn, TYPE_PUT), createFooter(value));
  }

  private Record createRecord(String key, long sn, byte type) {
    return new Record(createHeader(key, sn, type), emptyFooter());
  }

  private Record createRecord(String key, long sn, byte type, String value) {
    return new Record(createHeader(key, sn, type), createFooter(value));
  }


  private String readValue(Footer footer) {
    if (footer.valueSize() == 0) return "";
    byte[] bytes = footer.value().toArray(ValueLayout.JAVA_BYTE);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  private String readKey(Header header) {
    byte[] bytes = header.key().toArray(ValueLayout.JAVA_BYTE);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  // --- Original tests refactored for Record ---

  @Test
  void testInsertAndGetSingleElement() {
    skipList.insert(createRecord("key1", 1, "value1"));

    Record result = skipList.get(createHeader("key1", 1));

    assertNotNull(result);
    assertEquals(1, result.header().SN());
    assertEquals("value1", readValue(result.footer()));
  }

  @Test
  void testGetNonExistentKey() {
    skipList.insert(createRecord("key1", 1, "value1"));

    Record result = skipList.get(createHeader("key2", 1));

    assertNull(result);
  }

  @Test
  void testInsertMultipleElements() {
    skipList.insert(createRecord("apple", 1, "red"));
    skipList.insert(createRecord("banana", 2, "yellow"));
    skipList.insert(createRecord("cherry", 3, "dark red"));

    assertNotNull(skipList.get(createHeader("apple", 1)));
    assertNotNull(skipList.get(createHeader("banana", 2)));
    assertNotNull(skipList.get(createHeader("cherry", 3)));
  }

  @Test
  void testInsertDuplicateKeyAndSN() {
    skipList.insert(createRecord("key1", 1, "first"));
    skipList.insert(createRecord("key1", 1, "second"));

    Record result = skipList.get(createHeader("key1", 1));

    assertNotNull(result);
    assertEquals(1, result.header().SN());
    // Duplicate insert should be ignored, first value kept
    assertEquals("first", readValue(result.footer()));
  }

  @Test
  void testSameKeyDifferentSNDescendingOrder() {
    skipList.insert(createRecord("key1", 1, "v1"));
    skipList.insert(createRecord("key1", 3, "v3"));
    skipList.insert(createRecord("key1", 2, "v2"));

    Record result1 = skipList.get(createHeader("key1", 3));
    Record result2 = skipList.get(createHeader("key1", 2));
    Record result3 = skipList.get(createHeader("key1", 1));

    assertNotNull(result1);
    assertEquals(3, result1.header().SN());
    assertNotNull(result2);
    assertEquals(2, result2.header().SN());
    assertNotNull(result3);
    assertEquals(1, result3.header().SN());
  }

  @Test
  void testInsertInReverseOrder() {
    skipList.insert(createRecord("z", 1, "last"));
    skipList.insert(createRecord("m", 2, "middle"));
    skipList.insert(createRecord("a", 3, "first"));

    assertEquals(3, skipList.get(createHeader("a", 3)).header().SN());
    assertEquals(2, skipList.get(createHeader("m", 2)).header().SN());
    assertEquals(1, skipList.get(createHeader("z", 1)).header().SN());
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
      skipList.insert(createRecord("key" + String.format("%05d", i), i, "val" + i));
    }

    for (int i = 0; i < 100000; i++) {
      String key = "key" + String.format("%05d", i);
      Record result = skipList.get(createHeader(key, i));
      assertNotNull(result, "Key " + key + " should exist");
      assertEquals(i, result.header().SN());
    }
  }

  @Test
  void testEmptySkipListGet() {
    Record result = skipList.get(createHeader("anykey", 1));

    assertNull(result);
  }

  @Test
  void testEmptyKey() {
    skipList.insert(createRecord("", 1, "emptykey"));

    Record result = skipList.get(createHeader("", 1));

    assertNotNull(result);
    assertEquals(1, result.header().SN());
    assertEquals("emptykey", readValue(result.footer()));
  }

  @Test
  void testSingleCharacterKeys() {
    skipList.insert(createRecord("a", 1, "va"));
    skipList.insert(createRecord("b", 2, "vb"));
    skipList.insert(createRecord("c", 3, "vc"));

    assertEquals(1, skipList.get(createHeader("a", 1)).header().SN());
    assertEquals(2, skipList.get(createHeader("b", 2)).header().SN());
    assertEquals(3, skipList.get(createHeader("c", 3)).header().SN());
  }

  @Test
  void testVeryLongKey() {
    String longKey = "k".repeat(500);
    skipList.insert(createRecord(longKey, 1, "longkeyvalue"));

    Record result = skipList.get(createHeader(longKey, 1));

    assertNotNull(result);
    assertEquals(1, result.header().SN());
  }

  @Test
  void testKeysWithSamePrefix() {
    skipList.insert(createRecord("prefix", 1, "v1"));
    skipList.insert(createRecord("prefix1", 2, "v2"));
    skipList.insert(createRecord("prefix12", 3, "v3"));
    skipList.insert(createRecord("prefix123", 4, "v4"));

    assertEquals(1, skipList.get(createHeader("prefix", 1)).header().SN());
    assertEquals(2, skipList.get(createHeader("prefix1", 2)).header().SN());
    assertEquals(3, skipList.get(createHeader("prefix12", 3)).header().SN());
    assertEquals(4, skipList.get(createHeader("prefix123", 4)).header().SN());
  }

  @Test
  void testBinaryKeyData() {
    byte[] key1 = new byte[]{(byte) 0x00, (byte) 0x01};
    byte[] key2 = new byte[]{(byte) 0xFF, (byte) 0xFE};

    Header header1 = new Header(key1.length, MemorySegment.ofArray(key1), 1, TYPE_PUT);
    Header header2 = new Header(key2.length, MemorySegment.ofArray(key2), 2, TYPE_PUT);

    skipList.insert(new Record(header1, createFooter("bin1")));
    skipList.insert(new Record(header2, createFooter("bin2")));

    assertNotNull(skipList.get(new Header(key1.length, MemorySegment.ofArray(key1), 1, TYPE_PUT)));
    assertNotNull(skipList.get(new Header(key2.length, MemorySegment.ofArray(key2), 2, TYPE_PUT)));
  }

  @Test
  void testNegativeSN() {
    skipList.insert(createRecord("key", -1, "neg"));

    Record result = skipList.get(createHeader("key", -1));
    assertNotNull(result);
    assertEquals(-1, result.header().SN());
  }

  @Test
  void testMaxIntSN() {
    skipList.insert(createRecord("key", Integer.MAX_VALUE, "maxint"));

    Record result = skipList.get(createHeader("key", Integer.MAX_VALUE));
    assertNotNull(result);
    assertEquals(Integer.MAX_VALUE, result.header().SN());
  }

  @Test
  void testMaxLongSN() {
    skipList.insert(createRecord("key", Long.MAX_VALUE, "maxlong"));

    Record result = skipList.get(createHeader("key", Long.MAX_VALUE));
    assertNotNull(result);
    assertEquals(Long.MAX_VALUE, result.header().SN());
  }

  @Test
  void testMinLongSN() {
    skipList.insert(createRecord("key", Long.MIN_VALUE, "minlong"));

    Record result = skipList.get(createHeader("key", Long.MIN_VALUE));
    assertNotNull(result);
    assertEquals(Long.MIN_VALUE, result.header().SN());
  }

  @Test
  void testLongSNBeyondIntRange() {
    long largeSN = (long) Integer.MAX_VALUE + 100L;
    skipList.insert(createRecord("key", largeSN, "beyond"));

    Record result = skipList.get(createHeader("key", largeSN));
    assertNotNull(result);
    assertEquals(largeSN, result.header().SN());
  }

  @Test
  void testMultipleLongSNsForSameKey() {
    long sn1 = 1L;
    long sn2 = (long) Integer.MAX_VALUE + 1L;
    long sn3 = Long.MAX_VALUE;

    skipList.insert(createRecord("key", sn1, "v1"));
    skipList.insert(createRecord("key", sn2, "v2"));
    skipList.insert(createRecord("key", sn3, "v3"));

    assertEquals(sn1, skipList.get(createHeader("key", sn1)).header().SN());
    assertEquals(sn2, skipList.get(createHeader("key", sn2)).header().SN());
    assertEquals(sn3, skipList.get(createHeader("key", sn3)).header().SN());
  }

  @Test
  void testLongSNDescendingOrderWithinSameKey() {
    long sn1 = 1_000_000_000_000L;
    long sn2 = 2_000_000_000_000L;
    long sn3 = 3_000_000_000_000L;

    skipList.insert(createRecord("key", sn1, "v1"));
    skipList.insert(createRecord("key", sn3, "v3"));
    skipList.insert(createRecord("key", sn2, "v2"));

    Record r3 = skipList.get(createHeader("key", sn3));
    Record r2 = skipList.get(createHeader("key", sn2));
    Record r1 = skipList.get(createHeader("key", sn1));

    assertNotNull(r3);
    assertEquals(sn3, r3.header().SN());
    assertNotNull(r2);
    assertEquals(sn2, r2.header().SN());
    assertNotNull(r1);
    assertEquals(sn1, r1.header().SN());
  }

  @Test
  void testCompareHeadersLongSNDescending() {
    long highSN = 5_000_000_000L;
    long lowSN = 1_000_000_000L;

    Header a = createHeader("test", highSN);
    Header b = createHeader("test", lowSN);

    assertTrue(skipList.compareHeaders(a, b) < 0);
  }

  @Test
  void testCompareHeadersLongSNAscending() {
    long lowSN = 1_000_000_000L;
    long highSN = 5_000_000_000L;

    Header a = createHeader("test", lowSN);
    Header b = createHeader("test", highSN);

    assertTrue(skipList.compareHeaders(a, b) > 0);
  }

  @Test
  void testInsertionOrder() {
    String[] keys = {"delta", "alpha", "gamma", "beta"};
    for (int i = 0; i < keys.length; i++) {
      skipList.insert(createRecord(keys[i], i, "val" + i));
    }

    for (int i = 0; i < keys.length; i++) {
      assertNotNull(skipList.get(createHeader(keys[i], i)));
    }
  }

  @Test
  void testKeyWithNullBytes() {
    byte[] key = new byte[]{'a', 0x00, 'b', 0x00, 'c'};
    Header header = new Header(key.length, MemorySegment.ofArray(key), 1, TYPE_PUT);
    skipList.insert(new Record(header, createFooter("nullbytes")));

    Record result = skipList.get(new Header(key.length, MemorySegment.ofArray(key), 1, TYPE_PUT));
    assertNotNull(result);
    assertEquals(1, result.header().SN());
  }

  @Test
  void testVarintBoundary127() {
    String key127 = "x".repeat(127);
    skipList.insert(createRecord(key127, 1, "v127"));

    Record result = skipList.get(createHeader(key127, 1));
    assertNotNull(result);
  }

  @Test
  void testVarintBoundary128() {
    String key128 = "x".repeat(128);
    skipList.insert(createRecord(key128, 1, "v128"));

    Record result = skipList.get(createHeader(key128, 1));
    assertNotNull(result);
  }

  @Test
  void testConcurrentReads() throws InterruptedException {
    for (int i = 0; i < 100; i++) {
      skipList.insert(createRecord("key" + i, i, "val" + i));
    }

    int threadCount = 4;
    Thread[] threads = new Thread[threadCount];
    AtomicInteger failures = new AtomicInteger(0);

    for (int t = 0; t < threadCount; t++) {
      threads[t] = new Thread(() -> {
        for (int i = 0; i < 100; i++) {
          Record result = skipList.get(createHeader("key" + i, i));
          if (result == null || result.header().SN() != i) {
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
    skipList.insert(createRecord("testkey", 1, "testvalue"));
    int afterInsertSize = arena.getArenaSize();

    assertTrue(afterInsertSize > initialSize, "Arena size should increase after insert");
  }

  @Test
  void testMultipleVersionsOfSameKey() {
    skipList.insert(createRecord("user:1", 100, "v100"));
    skipList.insert(createRecord("user:1", 200, "v200"));
    skipList.insert(createRecord("user:1", 300, "v300"));

    assertNotNull(skipList.get(createHeader("user:1", 300)));
    assertNotNull(skipList.get(createHeader("user:1", 200)));
    assertNotNull(skipList.get(createHeader("user:1", 100)));
  }

  @Test
  void testSNOrderingWithMixedKeys() {
    skipList.insert(createRecord("a", 1, "a1"));
    skipList.insert(createRecord("a", 3, "a3"));
    skipList.insert(createRecord("b", 2, "b2"));
    skipList.insert(createRecord("a", 2, "a2"));

    assertEquals(3, skipList.get(createHeader("a", 3)).header().SN());
    assertEquals(2, skipList.get(createHeader("a", 2)).header().SN());
    assertEquals(1, skipList.get(createHeader("a", 1)).header().SN());
    assertEquals(2, skipList.get(createHeader("b", 2)).header().SN());
  }

  @Test
  void testPrintOrderedKeysAndSNs() {
    skipList.insert(createRecord("apple", 1, "a1"));
    skipList.insert(createRecord("apple", 3, "a3"));
    skipList.insert(createRecord("apple", 2, "a2"));
    skipList.insert(createRecord("banana", 5, "b5"));
    skipList.insert(createRecord("banana", 2, "b2"));
    skipList.insert(createRecord("cherry", 1, "c1"));

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
    skipList.insert(createRecord("key1", 1, TYPE_PUT, "putval"));

    Record result = skipList.get(createHeader("key1", 1, TYPE_PUT));
    assertNotNull(result);
    assertEquals(TYPE_PUT, result.header().type());
  }

  @Test
  void testDeleteTypeIsStoredAndRetrieved() {
    skipList.insert(createRecord("key1", 2, TYPE_DELETE));

    Record result = skipList.get(createHeader("key1", 2, TYPE_DELETE));
    assertNotNull(result);
    assertEquals(TYPE_DELETE, result.header().type());
  }

  @Test
  void testPutAndDeleteSameKeySameSN() {
    skipList.insert(createRecord("key1", 1, TYPE_PUT, "val"));
    skipList.insert(createRecord("key1", 1, TYPE_DELETE));

    Record result = skipList.get(createHeader("key1", 1, TYPE_PUT));
    assertNotNull(result);
    assertEquals(1, result.header().SN());
  }

  @Test
  void testPutFollowedByDeleteDifferentSN() {
    skipList.insert(createRecord("key1", 1, TYPE_PUT, "putval"));
    skipList.insert(createRecord("key1", 2, TYPE_DELETE));

    Record putResult = skipList.get(createHeader("key1", 1, TYPE_PUT));
    Record deleteResult = skipList.get(createHeader("key1", 2, TYPE_DELETE));

    assertNotNull(putResult);
    assertEquals(TYPE_PUT, putResult.header().type());
    assertEquals(1, putResult.header().SN());

    assertNotNull(deleteResult);
    assertEquals(TYPE_DELETE, deleteResult.header().type());
    assertEquals(2, deleteResult.header().SN());
  }

  @Test
  void testMultipleTypesAcrossKeys() {
    skipList.insert(createRecord("a", 1, TYPE_PUT, "va"));
    skipList.insert(createRecord("b", 2, TYPE_DELETE));
    skipList.insert(createRecord("c", 3, TYPE_PUT, "vc"));
    skipList.insert(createRecord("d", 4, TYPE_DELETE));

    assertEquals(TYPE_PUT, skipList.get(createHeader("a", 1)).header().type());
    assertEquals(TYPE_DELETE, skipList.get(createHeader("b", 2)).header().type());
    assertEquals(TYPE_PUT, skipList.get(createHeader("c", 3)).header().type());
    assertEquals(TYPE_DELETE, skipList.get(createHeader("d", 4)).header().type());
  }

  @Test
  void testTypePreservedWithMultipleVersions() {
    skipList.insert(createRecord("doc", 1, TYPE_PUT, "v1"));
    skipList.insert(createRecord("doc", 2, TYPE_PUT, "v2"));
    skipList.insert(createRecord("doc", 3, TYPE_DELETE));

    Record v1 = skipList.get(createHeader("doc", 1));
    Record v2 = skipList.get(createHeader("doc", 2));
    Record v3 = skipList.get(createHeader("doc", 3));

    assertNotNull(v1);
    assertEquals(TYPE_PUT, v1.header().type());
    assertNotNull(v2);
    assertEquals(TYPE_PUT, v2.header().type());
    assertNotNull(v3);
    assertEquals(TYPE_DELETE, v3.header().type());
  }

  @Test
  void testForEachIncludesType() {
    skipList.insert(createRecord("x", 1, TYPE_PUT, "xval"));
    skipList.insert(createRecord("y", 2, TYPE_DELETE));

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
      skipList.insert(createRecord("key", 1, type));

      Record result = skipList.get(createHeader("key", 1, type));
      assertNotNull(result, "Should find header with type=" + t);
      assertEquals(type, result.header().type(), "Type should match for value=" + t);

      arena.close();
    }
  }

  @Test
  void testTypeWithLongSN() {
    long largeSN = 9_999_999_999L;
    skipList.insert(createRecord("key1", largeSN, TYPE_PUT, "putval"));
    skipList.insert(createRecord("key2", largeSN, TYPE_DELETE));

    Record putResult = skipList.get(createHeader("key1", largeSN));
    Record deleteResult = skipList.get(createHeader("key2", largeSN));

    assertNotNull(putResult);
    assertEquals(TYPE_PUT, putResult.header().type());
    assertEquals(largeSN, putResult.header().SN());

    assertNotNull(deleteResult);
    assertEquals(TYPE_DELETE, deleteResult.header().type());
    assertEquals(largeSN, deleteResult.header().SN());
  }

  @Test
  void testDeleteThenPutDifferentSN() {
    skipList.insert(createRecord("key1", 1, TYPE_DELETE));
    skipList.insert(createRecord("key1", 2, TYPE_PUT, "restored"));

    Record v1 = skipList.get(createHeader("key1", 1));
    Record v2 = skipList.get(createHeader("key1", 2));

    assertNotNull(v1);
    assertEquals(TYPE_DELETE, v1.header().type());
    assertNotNull(v2);
    assertEquals(TYPE_PUT, v2.header().type());
  }

  @Test
  void testForEachOrderWithTypes() {
    skipList.insert(createRecord("a", 2, TYPE_DELETE));
    skipList.insert(createRecord("a", 1, TYPE_PUT, "aval"));
    skipList.insert(createRecord("b", 1, TYPE_PUT, "bval"));

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
      skipList.insert(createRecord("key" + String.format("%04d", i), i, type, "val" + i));
    }

    for (int i = 0; i < 1000; i++) {
      byte expectedType = (i % 2 == 0) ? TYPE_PUT : TYPE_DELETE;
      String key = "key" + String.format("%04d", i);
      Record result = skipList.get(createHeader(key, i));
      assertNotNull(result, "Key " + key + " should exist");
      assertEquals(expectedType, result.header().type(), "Type mismatch for key " + key);
    }
  }

  @Test
  void testLongSNWithNegativeValues() {
    long negativeSN = -5_000_000_000L;
    skipList.insert(createRecord("key", negativeSN, "negval"));

    Record result = skipList.get(createHeader("key", negativeSN));
    assertNotNull(result);
    assertEquals(negativeSN, result.header().SN());
  }

  @Test
  void testMixedLongSNsAcrossKeys() {
    skipList.insert(createRecord("alpha", Long.MIN_VALUE, "minval"));
    skipList.insert(createRecord("beta", 0L, "zeroval"));
    skipList.insert(createRecord("gamma", Long.MAX_VALUE, "maxval"));

    assertEquals(Long.MIN_VALUE, skipList.get(createHeader("alpha", Long.MIN_VALUE)).header().SN());
    assertEquals(0L, skipList.get(createHeader("beta", 0L)).header().SN());
    assertEquals(Long.MAX_VALUE, skipList.get(createHeader("gamma", Long.MAX_VALUE)).header().SN());
  }

  @Test
  void testConcurrentReadsWithTypes() throws InterruptedException {
    for (int i = 0; i < 100; i++) {
      byte type = (i % 2 == 0) ? TYPE_PUT : TYPE_DELETE;
      skipList.insert(createRecord("key" + i, i, type, "val" + i));
    }

    int threadCount = 4;
    Thread[] threads = new Thread[threadCount];
    AtomicInteger failures = new AtomicInteger(0);

    for (int t = 0; t < threadCount; t++) {
      threads[t] = new Thread(() -> {
        for (int i = 0; i < 100; i++) {
          byte expectedType = (i % 2 == 0) ? TYPE_PUT : TYPE_DELETE;
          Record result = skipList.get(createHeader("key" + i, i));
          if (result == null || result.header().SN() != i || result.header().type() != expectedType) {
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
    skipList.insert(createRecord("apple", 2, TYPE_DELETE));
    skipList.insert(createRecord("apple", 1, TYPE_PUT, "appleval"));
    skipList.insert(createRecord("banana", 3, TYPE_PUT, "bananaval"));
    skipList.insert(createRecord("banana", 1, TYPE_DELETE));

    System.out.println("=== SkipList Ordering with Types ===");
    skipList.forEach(header -> {
      byte[] keyBytes = header.key().toArray(ValueLayout.JAVA_BYTE);
      String key = new String(keyBytes, StandardCharsets.UTF_8);
      String typeStr = header.type() == TYPE_PUT ? "PUT" : "DELETE";
      System.out.println("  " + key + ":SN=" + header.SN() + ":" + typeStr);
    });
  }

  // =====================================================================
  // NEW FOOTER TESTS
  // =====================================================================

  @Test
  void testFooterValueStoredAndRetrieved() {
    skipList.insert(createRecord("key1", 1, "hello world"));

    Record result = skipList.get(createHeader("key1", 1));

    assertNotNull(result);
    assertEquals("hello world", readValue(result.footer()));
  }

  @Test
  void testFooterEmptyValue() {
    skipList.insert(createRecord("key1", 1, ""));

    Record result = skipList.get(createHeader("key1", 1));

    assertNotNull(result);
    assertEquals(0, result.footer().valueSize());
    assertEquals("", readValue(result.footer()));
  }

  @Test
  void testFooterValueSizeMatchesActual() {
    String value = "test-value-123";
    skipList.insert(createRecord("key1", 1, value));

    Record result = skipList.get(createHeader("key1", 1));

    assertNotNull(result);
    assertEquals(value.getBytes(StandardCharsets.UTF_8).length, result.footer().valueSize());
  }

  @Test
  void testFooterWithBinaryData() {
    byte[] binaryValue = new byte[]{(byte) 0x00, (byte) 0xFF, (byte) 0x7F, (byte) 0x80, (byte) 0x01};
    Header header = createHeader("binkey", 1);
    Footer footer = createFooter(binaryValue);
    skipList.insert(new Record(header, footer));

    Record result = skipList.get(createHeader("binkey", 1));

    assertNotNull(result);
    assertEquals(binaryValue.length, result.footer().valueSize());
    byte[] retrievedValue = result.footer().value().toArray(ValueLayout.JAVA_BYTE);
    assertArrayEquals(binaryValue, retrievedValue);
  }

  @Test
  void testFooterWithNullBytesInValue() {
    byte[] valueWithNulls = new byte[]{'h', 0x00, 'e', 0x00, 'l', 'l', 'o'};
    Header header = createHeader("nullval", 1);
    Footer footer = createFooter(valueWithNulls);
    skipList.insert(new Record(header, footer));

    Record result = skipList.get(createHeader("nullval", 1));

    assertNotNull(result);
    byte[] retrieved = result.footer().value().toArray(ValueLayout.JAVA_BYTE);
    assertArrayEquals(valueWithNulls, retrieved);
  }

  @Test
  void testFooterLargeValue() {
    String largeValue = "V".repeat(10000);
    skipList.insert(createRecord("bigval", 1, largeValue));

    Record result = skipList.get(createHeader("bigval", 1));

    assertNotNull(result);
    assertEquals(largeValue, readValue(result.footer()));
    assertEquals(10000, result.footer().valueSize());
  }

  @Test
  void testFooterVarintBoundary127Value() {
    String value127 = "v".repeat(127);
    skipList.insert(createRecord("k127", 1, value127));

    Record result = skipList.get(createHeader("k127", 1));
    assertNotNull(result);
    assertEquals(127, result.footer().valueSize());
    assertEquals(value127, readValue(result.footer()));
  }

  @Test
  void testFooterVarintBoundary128Value() {
    String value128 = "v".repeat(128);
    skipList.insert(createRecord("k128", 1, value128));

    Record result = skipList.get(createHeader("k128", 1));
    assertNotNull(result);
    assertEquals(128, result.footer().valueSize());
    assertEquals(value128, readValue(result.footer()));
  }

  @Test
  void testFooterVarintBoundary16383Value() {
    // 2-byte varint max is 16383 (0x3FFF)
    String value = "x".repeat(16383);
    skipList.insert(createRecord("kbig", 1, value));

    Record result = skipList.get(createHeader("kbig", 1));
    assertNotNull(result);
    assertEquals(16383, result.footer().valueSize());
    assertEquals(value, readValue(result.footer()));
  }

  @Test
  void testFooterVarintBoundary16384Value() {
    // 3-byte varint starts at 16384 (0x4000)
    String value = "x".repeat(16384);
    skipList.insert(createRecord("kbig2", 1, value));

    Record result = skipList.get(createHeader("kbig2", 1));
    assertNotNull(result);
    assertEquals(16384, result.footer().valueSize());
    assertEquals(value, readValue(result.footer()));
  }

  @Test
  void testFooterDifferentValuesForSameKeyDifferentSNs() {
    skipList.insert(createRecord("user", 1, "initial"));
    skipList.insert(createRecord("user", 2, "updated"));
    skipList.insert(createRecord("user", 3, "final"));

    Record r1 = skipList.get(createHeader("user", 1));
    Record r2 = skipList.get(createHeader("user", 2));
    Record r3 = skipList.get(createHeader("user", 3));

    assertNotNull(r1);
    assertEquals("initial", readValue(r1.footer()));
    assertNotNull(r2);
    assertEquals("updated", readValue(r2.footer()));
    assertNotNull(r3);
    assertEquals("final", readValue(r3.footer()));
  }

  @Test
  void testFooterPreservedAcrossMultipleKeys() {
    skipList.insert(createRecord("a", 1, "alpha-value"));
    skipList.insert(createRecord("b", 1, "beta-value"));
    skipList.insert(createRecord("c", 1, "gamma-value"));

    assertEquals("alpha-value", readValue(skipList.get(createHeader("a", 1)).footer()));
    assertEquals("beta-value", readValue(skipList.get(createHeader("b", 1)).footer()));
    assertEquals("gamma-value", readValue(skipList.get(createHeader("c", 1)).footer()));
  }

  @Test
  void testFooterPreservedAfterManyInserts() {
    for (int i = 0; i < 1000; i++) {
      skipList.insert(createRecord("key" + String.format("%04d", i), i, "value_" + i));
    }

    for (int i = 0; i < 1000; i++) {
      String key = "key" + String.format("%04d", i);
      Record result = skipList.get(createHeader(key, i));
      assertNotNull(result, "Key " + key + " should exist");
      assertEquals("value_" + i, readValue(result.footer()), "Value mismatch for key " + key);
    }
  }

  @Test
  void testFooterWithDeleteType() {
    // Delete records typically have empty values but should still work
    skipList.insert(createRecord("key1", 1, TYPE_DELETE));

    Record result = skipList.get(createHeader("key1", 1));
    assertNotNull(result);
    assertEquals(TYPE_DELETE, result.header().type());
    assertEquals(0, result.footer().valueSize());
  }

  @Test
  void testFooterWithDeleteTypeNonEmptyValue() {
    // Some implementations allow tombstone values
    skipList.insert(createRecord("key1", 1, TYPE_DELETE, "tombstone-reason"));

    Record result = skipList.get(createHeader("key1", 1));
    assertNotNull(result);
    assertEquals(TYPE_DELETE, result.header().type());
    assertEquals("tombstone-reason", readValue(result.footer()));
  }

  @Test
  void testRecordIntegrity_HeaderAndFooterMatch() {
    skipList.insert(createRecord("integrity", 42, TYPE_PUT, "the-value"));

    Record result = skipList.get(createHeader("integrity", 42));

    assertNotNull(result);
    // Verify header
    assertEquals(42, result.header().SN());
    assertEquals(TYPE_PUT, result.header().type());
    assertEquals("integrity", readKey(result.header()));
    // Verify footer
    assertEquals("the-value", readValue(result.footer()));
    assertEquals("the-value".length(), result.footer().valueSize());
  }

  @Test
  void testFooterNotCorruptedByAdjacentInserts() {
    // Insert records with varying key and value sizes to stress memory layout
    skipList.insert(createRecord("a", 1, "short"));
    skipList.insert(createRecord("bb", 2, "a-medium-length-value"));
    skipList.insert(createRecord("ccc", 3, "x".repeat(500)));
    skipList.insert(createRecord("dddd", 4, "tiny"));
    skipList.insert(createRecord("eeeee", 5, "y".repeat(1000)));

    assertEquals("short", readValue(skipList.get(createHeader("a", 1)).footer()));
    assertEquals("a-medium-length-value", readValue(skipList.get(createHeader("bb", 2)).footer()));
    assertEquals("x".repeat(500), readValue(skipList.get(createHeader("ccc", 3)).footer()));
    assertEquals("tiny", readValue(skipList.get(createHeader("dddd", 4)).footer()));
    assertEquals("y".repeat(1000), readValue(skipList.get(createHeader("eeeee", 5)).footer()));
  }

  @Test
  void testArenaMemoryIncreasesWithFooterSize() {
    int initialSize = arena.getArenaSize();

    skipList.insert(createRecord("k1", 1, "small"));
    int sizeAfterSmall = arena.getArenaSize();

    skipList.insert(createRecord("k2", 2, "a-much-larger-value-that-takes-more-space"));
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
      skipList.insert(createRecord("key" + i, i, "value_" + i));
    }

    int threadCount = 4;
    Thread[] threads = new Thread[threadCount];
    AtomicInteger failures = new AtomicInteger(0);

    for (int t = 0; t < threadCount; t++) {
      threads[t] = new Thread(() -> {
        for (int i = 0; i < 100; i++) {
          Record result = skipList.get(createHeader("key" + i, i));
          if (result == null) {
            failures.incrementAndGet();
            continue;
          }
          String expectedValue = "value_" + i;
          String actualValue = readValue(result.footer());
          if (!expectedValue.equals(actualValue)) {
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
    skipList.insert(createRecord("k", 1, TYPE_PUT, "x"));

    Record result = skipList.get(createHeader("k", 1));
    assertNotNull(result);
    assertEquals("x", readValue(result.footer()));
    assertEquals(1, result.footer().valueSize());
  }

  @Test
  void testFooterAllZeroBytesValue() {
    byte[] zeros = new byte[]{0, 0, 0, 0, 0};
    skipList.insert(new Record(createHeader("zeros", 1), createFooter(zeros)));

    Record result = skipList.get(createHeader("zeros", 1));
    assertNotNull(result);
    byte[] retrieved = result.footer().value().toArray(ValueLayout.JAVA_BYTE);
    assertArrayEquals(zeros, retrieved);
  }

  @Test
  void testFooterAllOxFFBytesValue() {
    byte[] ffs = new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
    skipList.insert(new Record(createHeader("ffs", 1), createFooter(ffs)));

    Record result = skipList.get(createHeader("ffs", 1));
    assertNotNull(result);
    byte[] retrieved = result.footer().value().toArray(ValueLayout.JAVA_BYTE);
    assertArrayEquals(ffs, retrieved);
  }

  @Test
  void testFooterValueWithSpecialCharacters() {
    String specialValue = "héllo wörld 日本語 🚀";
    skipList.insert(createRecord("special", 1, specialValue));

    Record result = skipList.get(createHeader("special", 1));
    assertNotNull(result);
    assertEquals(specialValue, readValue(result.footer()));
  }

  @Test
  void testDuplicateInsertPreservesOriginalFooter() {
    skipList.insert(createRecord("dup", 1, "original-value"));
    skipList.insert(createRecord("dup", 1, "new-value-should-be-ignored"));

    Record result = skipList.get(createHeader("dup", 1));
    assertNotNull(result);
    // The skip list ignores duplicate (key,SN) inserts, so original footer preserved
    assertEquals("original-value", readValue(result.footer()));
  }

  @Test
  void testFooterWithEmptyKeyAndValue() {
    skipList.insert(createRecord("", 1, ""));

    Record result = skipList.get(createHeader("", 1));
    assertNotNull(result);
    assertEquals(0, result.header().keySize());
    assertEquals(0, result.footer().valueSize());
  }

  @Test
  void testFooterWithLongKeyAndLongValue() {
    String longKey = "K".repeat(500);
    String longValue = "V".repeat(5000);
    skipList.insert(createRecord(longKey, 1, longValue));

    Record result = skipList.get(createHeader(longKey, 1));
    assertNotNull(result);
    assertEquals(longKey, readKey(result.header()));
    assertEquals(longValue, readValue(result.footer()));
  }

  @Test
  void testFooterRetrievedCorrectlyInForEachTraversal() {
    skipList.insert(createRecord("alpha", 1, "alpha-val"));
    skipList.insert(createRecord("beta", 2, "beta-val"));
    skipList.insert(createRecord("gamma", 3, "gamma-val"));

    // forEach only gives Header, but we can verify via get
    skipList.forEach(header -> {
      String key = readKey(header);
      Record record = skipList.get(header);
      assertNotNull(record, "Record should exist for key " + key);
      assertNotNull(record.footer(), "Footer should not be null for key " + key);
      assertTrue(record.footer().valueSize() > 0, "Footer value size should be > 0 for key " + key);
    });
  }

  @Test
  void testMultipleVersionsWithDifferentFooters() {
    skipList.insert(createRecord("doc", 1, TYPE_PUT, "version-1-content"));
    skipList.insert(createRecord("doc", 2, TYPE_PUT, "version-2-content-updated"));
    skipList.insert(createRecord("doc", 3, TYPE_DELETE, ""));

    Record v1 = skipList.get(createHeader("doc", 1));
    Record v2 = skipList.get(createHeader("doc", 2));
    Record v3 = skipList.get(createHeader("doc", 3));

    assertNotNull(v1);
    assertEquals("version-1-content", readValue(v1.footer()));
    assertNotNull(v2);
    assertEquals("version-2-content-updated", readValue(v2.footer()));
    assertNotNull(v3);
    assertEquals("", readValue(v3.footer()));
    assertEquals(TYPE_DELETE, v3.header().type());
  }

  @Test
  void testFooterValueExactly255Bytes() {
    // 255 = 0xFF, edge case for single-byte representations
    String value = "Z".repeat(255);
    skipList.insert(createRecord("k255", 1, value));

    Record result = skipList.get(createHeader("k255", 1));
    assertNotNull(result);
    assertEquals(255, result.footer().valueSize());
    assertEquals(value, readValue(result.footer()));
  }

  @Test
  void testFooterValueExactly256Bytes() {
    String value = "Z".repeat(256);
    skipList.insert(createRecord("k256", 1, value));

    Record result = skipList.get(createHeader("k256", 1));
    assertNotNull(result);
    assertEquals(256, result.footer().valueSize());
    assertEquals(value, readValue(result.footer()));
  }

  @Test
  void testGetReturnsFooterNotInputHeader() {
    // The get method should return the stored record's footer, not reconstruct from the search header
    Header searchHeader = createHeader("mykey", 10);
    skipList.insert(createRecord("mykey", 10, TYPE_PUT, "stored-value"));

    Record result = skipList.get(searchHeader);
    assertNotNull(result);
    // The returned record should contain the stored footer
    assertEquals("stored-value", readValue(result.footer()));
    // And the returned header should match
    assertEquals(10, result.header().SN());
  }

  @Test
  void testInsertAndGetRecordWithMaxVarintKeyAndValue() {
    // Both key and value at varint boundary
    String key = "k".repeat(128);
    String value = "v".repeat(128);
    skipList.insert(createRecord(key, 1, value));

    Record result = skipList.get(createHeader(key, 1));
    assertNotNull(result);
    assertEquals(128, result.header().keySize());
    assertEquals(128, result.footer().valueSize());
    assertEquals(key, readKey(result.header()));
    assertEquals(value, readValue(result.footer()));
  }

  @Test
  void testFooterCorrectAfterReverseOrderInserts() {
    // Insert in reverse key order to test skip list rebalancing with footers
    for (int i = 99; i >= 0; i--) {
      skipList.insert(createRecord("key" + String.format("%02d", i), i, "val" + i));
    }

    for (int i = 0; i < 100; i++) {
      String key = "key" + String.format("%02d", i);
      Record result = skipList.get(createHeader(key, i));
      assertNotNull(result, "Key " + key + " should exist");
      assertEquals("val" + i, readValue(result.footer()), "Value mismatch for key " + key);
    }
  }
}

