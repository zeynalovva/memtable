package az.zeynalov.tests;

import az.zeynalov.memtable.Arena;
import az.zeynalov.memtable.Footer;
import az.zeynalov.memtable.Header;
import az.zeynalov.memtable.SkipList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SkipListTest {

  private static final int NODE_ARRAY_LENGTH_SIZE = Integer.BYTES;
  private static final int NODE_POINTER_SIZE = Integer.BYTES;
  private Arena arena;
  private SkipList skipList;
  private java.lang.foreign.Arena testScope;

  @BeforeEach
  void setUp() {
    arena = new Arena();
    skipList = new SkipList(arena);
    skipList.init();
    testScope = java.lang.foreign.Arena.ofShared();
  }

  @AfterEach
  void tearDown() {
    arena.close();
    if (testScope.scope().isAlive()) {
      testScope.close();
    }
  }

  @Test
  void testInitialStateAndFirstOffset() {
    // The head node is created with MAX_LEVEL (12) upon init().
    // Size = Array length (4 bytes) + 12 pointers (12 * 4 = 48 bytes) = 52 bytes.
    // Therefore, the exact offset for the very first insertion must be 52.

    Header header = createHeader("Key1", 1L, (byte) 1);
    Footer footer = createFooter("Val1");

    skipList.insert(header, footer);

    int offset = skipList.get(header);

    // Verifying the exact offset mathematically
    assertEquals(52, offset, "The first inserted node should be at exactly offset 52");
  }

  @Test
  void testExactNodeMemoryLayoutAtOffset() {
    String keyStr = "LayoutKey";
    String valStr = "LayoutValue";
    Header header = createHeader(keyStr, 999L, (byte) 5);
    Footer footer = createFooter(valStr);

    skipList.insert(header, footer);
    int offset = skipList.get(header);

    assertTrue(offset > 0);

    // Manually unpacking the memory at the specific offset to verify correctness
    int levels = arena.readInt(offset);
    assertTrue(levels >= 1 && levels <= 13); // Levels are 1 to 13 (randomLevel + 1)

    int nodePointersSize = Integer.BYTES + (levels * Integer.BYTES);
    int currentOffset = offset + nodePointersSize;

    // 1. Check Key Varint Size
    long packedKeyVarint = arena.readVarint(currentOffset);
    int keySize = (int) (packedKeyVarint >>> 32);
    int keyVarintBytes = (int) (packedKeyVarint & 0xFFFFFFFFL);
    assertEquals(keyStr.length(), keySize);
    currentOffset += keyVarintBytes;

    // 2. Check Key Segment
    MemorySegment keySeg = arena.readBytes(currentOffset, keySize);
    assertEquals(keyStr, new String(keySeg.toArray(ValueLayout.JAVA_BYTE)));
    currentOffset += keySize;

    // 3. Check Sequence Number (SN)
    long sn = arena.readLong(currentOffset);
    assertEquals(999L, sn);
    currentOffset += Long.BYTES;

    // 4. Check Type
    byte type = arena.readByte(currentOffset);
    assertEquals((byte) 5, type);
    currentOffset += Byte.BYTES;

    // 5. Check Value Varint Size
    long packedValVarint = arena.readVarint(currentOffset);
    int valSize = (int) (packedValVarint >>> 32);
    int valVarintBytes = (int) (packedValVarint & 0xFFFFFFFFL);
    assertEquals(valStr.length(), valSize);
    currentOffset += valVarintBytes;

    // 6. Check Value Segment
    MemorySegment valSeg = arena.readBytes(currentOffset, valSize);
    assertEquals(valStr, new String(valSeg.toArray(ValueLayout.JAVA_BYTE)));
  }

  @Test
  void testMultipleInsertionsAndRetrievalOrder() {
    // Insert keys out of alphabetical order
    Header hA = createHeader("A", 1L, (byte) 1); Footer fA = createFooter("valA");
    Header hB = createHeader("B", 1L, (byte) 1); Footer fB = createFooter("valB");
    Header hC = createHeader("C", 1L, (byte) 1); Footer fC = createFooter("valC");

    skipList.insert(hB, fB);
    skipList.insert(hC, fC);
    skipList.insert(hA, fA);

    int offsetA = skipList.get(hA);
    int offsetB = skipList.get(hB);
    int offsetC = skipList.get(hC);

    List<Integer> offsetsFromIterator = new ArrayList<>();
    skipList.forEach(offsetsFromIterator::add);

    // Verify iteration guarantees lexicographical ascending order for offsets
    assertEquals(3, offsetsFromIterator.size());
    assertEquals(offsetA, offsetsFromIterator.get(0));
    assertEquals(offsetB, offsetsFromIterator.get(1));
    assertEquals(offsetC, offsetsFromIterator.get(2));
  }

  @Test
  void testSameKeyDescendingSequenceNumberOrder() {
    // In LSM trees, later Sequence Numbers (SN) should appear first to shadow old versions.
    // Let's verify our custom SN comparison results in descending SN order for identical keys.
    Header h10 = createHeader("SharedKey", 10L, (byte) 1); Footer f10 = createFooter("v10");
    Header h20 = createHeader("SharedKey", 20L, (byte) 1); Footer f20 = createFooter("v20");
    Header h30 = createHeader("SharedKey", 30L, (byte) 1); Footer f30 = createFooter("v30");

    skipList.insert(h20, f20);
    skipList.insert(h10, f10);
    skipList.insert(h30, f30);

    int o10 = skipList.get(h10);
    int o20 = skipList.get(h20);
    int o30 = skipList.get(h30);

    List<Integer> offsets = new ArrayList<>();
    skipList.forEach(offsets::add);

    assertEquals(3, offsets.size());

    // Expected order for same key: Highest SN first (Descending)
    assertEquals(o30, offsets.get(0));
    assertEquals(o20, offsets.get(1));
    assertEquals(o10, offsets.get(2));
  }

  @Test
  void testGetMissingKeyReturnsMinusOne() {
    skipList.insert(createHeader("ExistingKey", 1L, (byte) 1), createFooter("Val"));

    Header missingHeader = createHeader("MissingKey", 1L, (byte) 1);
    int offset = skipList.get(missingHeader);

    assertEquals(-1, offset, "Getting a missing key should safely return -1");
  }

  @Test
  void testIdempotencyOnExactDuplicateInsert() {
    // Insert same exact Key AND SN twice
    Header header = createHeader("DuplicateKey", 100L, (byte) 1);
    Footer footer1 = createFooter("Val1");
    Footer footer2 = createFooter("Val2");

    skipList.insert(header, footer1);
    int firstOffset = skipList.get(header);

    // Attempt second insert. Code logic should reject this because CompareNodeWithTarget == 0
    skipList.insert(header, footer2);
    int secondOffset = skipList.get(header);

    assertEquals(firstOffset, secondOffset, "Offsets should point to the exact same node address");

    List<Integer> offsets = new ArrayList<>();
    skipList.forEach(offsets::add);

    // Ensures no duplicated node was created locally in the list logic
    assertEquals(1, offsets.size(), "SkipList should only contain 1 element");
  }

  @Test
  void testLargePayloadVarintScaling() {
    // Create strings > 127 bytes to ensure varint consumes more than 1 byte encoding
    String largeString = "A".repeat(200);

    Header h = createHeader(largeString, 1L, (byte) 2);
    Footer f = createFooter(largeString);

    skipList.insert(h, f);
    int offset = skipList.get(h);
    assertTrue(offset > 0);

    int levels = arena.readInt(offset);
    int metaOffset = offset + Integer.BYTES + (levels * Integer.BYTES);

    long packedKeyVarint = arena.readVarint(metaOffset);
    int keySize = (int) (packedKeyVarint >>> 32);
    int varintBytes = (int) (packedKeyVarint & 0xFFFFFFFFL);

    assertEquals(200, keySize, "Decoded varint size should equal original payload size");
    assertEquals(2, varintBytes, "A varint for size 200 should safely consume 2 bytes in memory");
  }

  @Test
  void testIfReturnsNextNodePointerForNonExistingKey() {
    Header hA = createHeader("A", 10, (byte) 1); Footer fA = createFooter("valA");
    Header hB = createHeader("A", 12, (byte) 1); Footer fB = createFooter("valB");
    Header hC = createHeader("A", 8, (byte) 1); Footer fC = createFooter("valC");

    skipList.insert(hA, fA);
    skipList.insert(hC, fC);
    skipList.insert(hB, fB);

    Header wanted = createHeader("A", 9, (byte) 1);

    int offset = skipList.get(wanted);
    Header result = getHeader(offset);
    assertEquals("A", new String(result.key().toArray(ValueLayout.JAVA_BYTE)));
    assertEquals(8, result.SN());
  }

  // --- Helpers to convert String -> Off-heap MemorySegments ---

  private Header createHeader(String key, long sn, byte type) {
    MemorySegment keySegment = allocateOffHeapString(key);
    return new Header((int) keySegment.byteSize(), keySegment, sn, type);
  }

  private Footer createFooter(String value) {
    MemorySegment valSegment = allocateOffHeapString(value);
    return new Footer((int) valSegment.byteSize(), valSegment);
  }

  private Header getHeader(int offsetOfNode) {
    int headerOffset = skipNextNodePointers(offsetOfNode);
    long keyPayload = arena.readVarint(headerOffset);

    int keySize = unpackFirst(keyPayload);
    int keyOffset = headerOffset + unpackSecond(keyPayload);
    int SN_Size = Long.BYTES;

    MemorySegment key = arena.readBytes(keyOffset, keySize);
    int SN_Offset = keyOffset + keySize;
    long SN = arena.readLong(SN_Offset);
    int typeOffset = SN_Offset + SN_Size;
    byte type = arena.readByte(typeOffset);

    return new Header(keySize, key, SN, type);
  }

  private int skipNextNodePointers(int offsetOfNode) {
    int sizeOfNodes = arena.readInt(offsetOfNode);
    return offsetOfNode + NODE_ARRAY_LENGTH_SIZE + NODE_POINTER_SIZE * sizeOfNodes;
  }

  private int unpackFirst(long packed) {
    return (int) (packed >> 32);
  }

  private int unpackSecond(long packed) {
    return (int) packed;
  }

  private MemorySegment allocateOffHeapString(String str) {
    byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
    MemorySegment segment = testScope.allocate(bytes.length);
    MemorySegment.copy(MemorySegment.ofArray(bytes), 0, segment, 0, bytes.length);
    return segment;
  }

  @Test
  void testGetOnEmptySkipListReturnsMinusOne() {
    Header header = createHeader("AnyKey", 1L, (byte) 1);
    int offset = skipList.get(header);
    assertEquals(-1, offset, "Get on empty skip list should return -1");
  }

  @Test
  void testForEachOnEmptySkipListYieldsNothing() {
    List<Integer> offsets = new ArrayList<>();
    skipList.forEach(offsets::add);
    assertTrue(offsets.isEmpty(), "forEach on empty skip list should yield no elements");
  }

  @Test
  void testSingleInsertAndForEach() {
    Header h = createHeader("OnlyKey", 5L, (byte) 0);
    Footer f = createFooter("OnlyValue");

    skipList.insert(h, f);

    List<Integer> offsets = new ArrayList<>();
    skipList.forEach(offsets::add);

    assertEquals(1, offsets.size(), "ForEach should yield exactly one element");
    assertEquals(skipList.get(h), offsets.get(0));
  }

  @Test
  void testForEachOrderWithMultipleKeysAndMultipleSNs() {
    // Insert multiple keys each with multiple sequence numbers
    Header hA10 = createHeader("Apple", 10L, (byte) 1); Footer fA10 = createFooter("a10");
    Header hA20 = createHeader("Apple", 20L, (byte) 1); Footer fA20 = createFooter("a20");
    Header hB5  = createHeader("Banana", 5L, (byte) 1); Footer fB5  = createFooter("b5");
    Header hB15 = createHeader("Banana", 15L, (byte) 1); Footer fB15 = createFooter("b15");

    skipList.insert(hB5, fB5);
    skipList.insert(hA10, fA10);
    skipList.insert(hB15, fB15);
    skipList.insert(hA20, fA20);

    List<Integer> offsets = new ArrayList<>();
    skipList.forEach(offsets::add);

    assertEquals(4, offsets.size());

    // Expected order: Apple(SN=20), Apple(SN=10), Banana(SN=15), Banana(SN=5)
    assertEquals(skipList.get(hA20), offsets.get(0));
    assertEquals(skipList.get(hA10), offsets.get(1));
    assertEquals(skipList.get(hB15), offsets.get(2));
    assertEquals(skipList.get(hB5), offsets.get(3));
  }

  @Test
  void testEmptyKeyAndValue() {
    Header h = createHeader("", 1L, (byte) 0);
    Footer f = createFooter("");

    skipList.insert(h, f);
    int offset = skipList.get(h);
    assertTrue(offset > 0, "Empty key/value should still be inserted");

    // Verify we can read back the header
    Header result = getHeader(offset);
    assertEquals("", new String(result.key().toArray(ValueLayout.JAVA_BYTE)));
    assertEquals(1L, result.SN());
    assertEquals((byte) 0, result.type());
  }

  @Test
  void testTypeFieldIsPreservedCorrectly() {
    // Insert nodes with different type byte values and verify they are stored properly
    byte[] types = {0, 1, 2, 127, (byte) 255};
    for (byte type : types) {
      Arena localArena = new Arena();
      SkipList localList = new SkipList(localArena);
      localList.init();

      Header h = createHeader("Key", 1L, type);
      Footer f = createFooter("Val");
      localList.insert(h, f);

      int offset = localList.get(h);
      Header result = getHeaderFromArena(offset, localArena);
      assertEquals(type, result.type(), "Type byte should be preserved for value: " + type);
      localArena.close();
    }
  }

  @Test
  void testMVCCGetReturnsClosestLowerOrEqualSN() {
    // Insert same key with SNs: 10, 20, 30
    Header h10 = createHeader("Key", 10L, (byte) 1); Footer f10 = createFooter("v10");
    Header h20 = createHeader("Key", 20L, (byte) 1); Footer f20 = createFooter("v20");
    Header h30 = createHeader("Key", 30L, (byte) 1); Footer f30 = createFooter("v30");

    skipList.insert(h10, f10);
    skipList.insert(h20, f20);
    skipList.insert(h30, f30);

    // Search for SN=25 -> should return the node with SN=20 (closest lower or equal)
    Header search25 = createHeader("Key", 25L, (byte) 1);
    int offset = skipList.get(search25);
    assertNotEquals(-1, offset);
    Header result = getHeader(offset);
    assertEquals("Key", new String(result.key().toArray(ValueLayout.JAVA_BYTE)));
    assertEquals(20, result.SN());

    // Search for SN=30 -> should return exact match SN=30
    int offset30 = skipList.get(h30);
    assertNotEquals(-1, offset30);
    Header result30 = getHeader(offset30);
    assertEquals(30, result30.SN());

    // Search for SN=5 -> should return -1 (no SN <= 5 exists, because order is descending)
    // Actually based on the ordering (descending SN), searching SN=5 means it falls after SN=10
    // The get returns the next node pointer at level 0, which would be the node after the last match
    Header search5 = createHeader("Key", 5L, (byte) 1);
    int offset5 = skipList.get(search5);
    // SN=5 < all existing SNs, so it falls after SN=10 in descending order
    // The get should return SN=10 (since 10 is the next node)
    assertEquals(-1, offset5);
  }

  @Test
  void testMVCCGetWithSNHigherThanAll() {
    Header h10 = createHeader("Key", 10L, (byte) 1); Footer f10 = createFooter("v10");
    Header h20 = createHeader("Key", 20L, (byte) 1); Footer f20 = createFooter("v20");

    skipList.insert(h10, f10);
    skipList.insert(h20, f20);

    // Search SN=50, higher than all existing -> should return SN=20 (the highest, first in order)
    Header search50 = createHeader("Key", 50L, (byte) 1);
    int offset = skipList.get(search50);
    Header result = getHeader(offset);
    assertEquals("Key", new String(result.key().toArray(ValueLayout.JAVA_BYTE)));
    assertEquals(20L, result.SN());
  }

  @Test
  void testLargeNumberOfInsertions() {
    int count = 500;
    for (int i = 0; i < count; i++) {
      String key = String.format("key_%05d", i);
      Header h = createHeader(key, 1L, (byte) 1);
      Footer f = createFooter("value_" + i);
      skipList.insert(h, f);
    }

    // Verify all keys can be retrieved
    for (int i = 0; i < count; i++) {
      String key = String.format("key_%05d", i);
      Header h = createHeader(key, 1L, (byte) 1);
      int offset = skipList.get(h);
      assertTrue(offset > 0, "Key " + key + " should be found");
    }

    // Verify iteration count
    List<Integer> offsets = new ArrayList<>();
    skipList.forEach(offsets::add);
    assertEquals(count, offsets.size(), "All " + count + " keys should be iterated");
  }

  @Test
  void testIterationIsInLexicographicalOrder() {
    String[] keys = {"Zebra", "Mango", "Apple", "Banana", "Cherry"};
    for (String key : keys) {
      skipList.insert(createHeader(key, 1L, (byte) 1), createFooter("v"));
    }

    List<Integer> offsets = new ArrayList<>();
    skipList.forEach(offsets::add);

    assertEquals(keys.length, offsets.size());

    // Verify keys come out in sorted order
    String previous = null;
    for (int offset : offsets) {
      Header h = getHeader(offset);
      String currentKey = new String(h.key().toArray(ValueLayout.JAVA_BYTE));
      if (previous != null) {
        assertTrue(previous.compareTo(currentKey) < 0,
            "Keys should be in ascending order but got " + previous + " before " + currentKey);
      }
      previous = currentKey;
    }
  }

  @Test
  void testKeysWithCommonPrefix() {
    Header h1 = createHeader("prefix_a", 1L, (byte) 1); Footer f1 = createFooter("v1");
    Header h2 = createHeader("prefix_ab", 1L, (byte) 1); Footer f2 = createFooter("v2");
    Header h3 = createHeader("prefix_abc", 1L, (byte) 1); Footer f3 = createFooter("v3");
    Header h4 = createHeader("prefix_b", 1L, (byte) 1); Footer f4 = createFooter("v4");

    skipList.insert(h3, f3);
    skipList.insert(h1, f1);
    skipList.insert(h4, f4);
    skipList.insert(h2, f2);

    List<Integer> offsets = new ArrayList<>();
    skipList.forEach(offsets::add);

    assertEquals(4, offsets.size());
    assertEquals(skipList.get(h1), offsets.get(0));
    assertEquals(skipList.get(h2), offsets.get(1));
    assertEquals(skipList.get(h3), offsets.get(2));
    assertEquals(skipList.get(h4), offsets.get(3));
  }

  @Test
  void testValueCanBeReadBackFromOffset() {
    String keyStr = "ReadBackKey";
    String valStr = "ReadBackValue";
    Header h = createHeader(keyStr, 42L, (byte) 3);
    Footer f = createFooter(valStr);

    skipList.insert(h, f);
    int offset = skipList.get(h);

    // Navigate through the node to read the value
    int levels = arena.readInt(offset);
    int currentOffset = offset + NODE_ARRAY_LENGTH_SIZE + (levels * NODE_POINTER_SIZE);

    // Skip key varint + key
    long packedKeyVarint = arena.readVarint(currentOffset);
    int keySize = (int) (packedKeyVarint >>> 32);
    int keyVarintBytes = (int) (packedKeyVarint & 0xFFFFFFFFL);
    currentOffset += keyVarintBytes + keySize;

    // Skip SN + type
    currentOffset += Long.BYTES + Byte.BYTES;

    // Read value varint + value
    long packedValVarint = arena.readVarint(currentOffset);
    int valSize = (int) (packedValVarint >>> 32);
    int valVarintBytes = (int) (packedValVarint & 0xFFFFFFFFL);
    currentOffset += valVarintBytes;

    MemorySegment valSeg = arena.readBytes(currentOffset, valSize);
    String retrievedValue = new String(valSeg.toArray(ValueLayout.JAVA_BYTE));
    assertEquals(valStr, retrievedValue, "Value should be correctly stored and retrievable");
  }

  @Test
  void testSingleCharacterKeys() {
    Header hZ = createHeader("Z", 1L, (byte) 1); Footer fZ = createFooter("vZ");
    Header hA = createHeader("A", 1L, (byte) 1); Footer fA = createFooter("vA");
    Header hM = createHeader("M", 1L, (byte) 1); Footer fM = createFooter("vM");

    skipList.insert(hZ, fZ);
    skipList.insert(hA, fA);
    skipList.insert(hM, fM);

    List<Integer> offsets = new ArrayList<>();
    skipList.forEach(offsets::add);

    assertEquals(3, offsets.size());
    assertEquals(skipList.get(hA), offsets.get(0));
    assertEquals(skipList.get(hM), offsets.get(1));
    assertEquals(skipList.get(hZ), offsets.get(2));
  }

  @Test
  void testGetForNonExistingKeyBetweenExistingKeys() {
    Header hA = createHeader("A", 1L, (byte) 1); Footer fA = createFooter("vA");
    Header hC = createHeader("C", 1L, (byte) 1); Footer fC = createFooter("vC");
    Header hE = createHeader("E", 1L, (byte) 1); Footer fE = createFooter("vE");

    skipList.insert(hA, fA);
    skipList.insert(hC, fC);
    skipList.insert(hE, fE);

    // Search for "B" which doesn't exist - should return the next node ("C")
    Header searchB = createHeader("B", 1L, (byte) 1);
    int offset = skipList.get(searchB);
    Header result = getHeader(offset);
    assertEquals("C", new String(result.key().toArray(ValueLayout.JAVA_BYTE)),
        "Searching for non-existing key 'B' should return the next key 'C'");
  }

  @Test
  void testGetForKeyGreaterThanAllExistingReturnsMinusOne() {
    Header hA = createHeader("A", 1L, (byte) 1); Footer fA = createFooter("vA");
    Header hB = createHeader("B", 1L, (byte) 1); Footer fB = createFooter("vB");

    skipList.insert(hA, fA);
    skipList.insert(hB, fB);

    // Search for "Z" which is greater than all existing keys
    Header searchZ = createHeader("Z", 1L, (byte) 1);
    int offset = skipList.get(searchZ);
    assertEquals(-1, offset, "Searching for key greater than all should return -1");
  }

  @Test
  void testVarintBoundaryAt128() {
    // 127 fits in 1 varint byte, 128 requires 2 varint bytes
    String key127 = "K".repeat(127);
    String key128 = "K".repeat(128);

    Header h127 = createHeader(key127, 1L, (byte) 1); Footer f127 = createFooter("v");
    Header h128 = createHeader(key128, 1L, (byte) 1); Footer f128 = createFooter("v");

    skipList.insert(h127, f127);
    skipList.insert(h128, f128);

    int offset127 = skipList.get(h127);
    int offset128 = skipList.get(h128);

    assertTrue(offset127 > 0);
    assertTrue(offset128 > 0);

    // Verify varint sizes
    int levels127 = arena.readInt(offset127);
    int metaOffset127 = offset127 + NODE_ARRAY_LENGTH_SIZE + (levels127 * NODE_POINTER_SIZE);
    long packed127 = arena.readVarint(metaOffset127);
    assertEquals(127, (int) (packed127 >>> 32));
    assertEquals(1, (int) (packed127 & 0xFFFFFFFFL), "Varint for 127 should be 1 byte");

    int levels128 = arena.readInt(offset128);
    int metaOffset128 = offset128 + NODE_ARRAY_LENGTH_SIZE + (levels128 * NODE_POINTER_SIZE);
    long packed128 = arena.readVarint(metaOffset128);
    assertEquals(128, (int) (packed128 >>> 32));
    assertEquals(2, (int) (packed128 & 0xFFFFFFFFL), "Varint for 128 should be 2 bytes");
  }

  @Test
  void testSequenceNumberZero() {
    Header h = createHeader("Key", 0L, (byte) 1);
    Footer f = createFooter("Val");

    skipList.insert(h, f);
    int offset = skipList.get(h);
    assertTrue(offset > 0);

    Header result = getHeader(offset);
    assertEquals(0L, result.SN(), "SN of 0 should be stored and retrievable");
  }

  @Test
  void testLargeSequenceNumber() {
    long largeSN = Long.MAX_VALUE;
    Header h = createHeader("Key", largeSN, (byte) 1);
    Footer f = createFooter("Val");

    skipList.insert(h, f);
    int offset = skipList.get(h);
    assertTrue(offset > 0);

    Header result = getHeader(offset);
    assertEquals(largeSN, result.SN(), "Long.MAX_VALUE SN should be stored and retrievable");
  }

  private Header getHeaderFromArena(int offsetOfNode, Arena customArena) {
    int sizeOfNodes = customArena.readInt(offsetOfNode);
    int headerOffset = offsetOfNode + NODE_ARRAY_LENGTH_SIZE + NODE_POINTER_SIZE * sizeOfNodes;
    long keyPayload = customArena.readVarint(headerOffset);

    int keySize = (int) (keyPayload >> 32);
    int keyOffset = headerOffset + (int) (keyPayload & 0xFFFFFFFFL);

    MemorySegment key = customArena.readBytes(keyOffset, keySize);
    long SN = customArena.readLong(keyOffset + keySize);
    byte type = customArena.readByte(keyOffset + keySize + Long.BYTES);

    return new Header(keySize, key, SN, type);
  }
}