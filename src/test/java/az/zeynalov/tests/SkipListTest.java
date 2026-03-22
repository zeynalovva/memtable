package az.zeynalov.tests;

import az.zeynalov.memtable.Arena;
import az.zeynalov.memtable.SkipList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static az.zeynalov.memtable.SkipList.COLD_ARENA_POINTER_OFFSET;
import static org.junit.jupiter.api.Assertions.*;

class SkipListTest {

  private Arena hotArena;
  private Arena coldArena;
  private SkipList skipList;
  private java.lang.foreign.Arena testScope;

  private final static int PREFIX_LENGTH = 8;
  private final static int SN_LENGTH = 8;
  private final static int TYPE_LENGTH = 4;
  private final static int KEY_LENGTH = 4;
  private final static int VALUE_LENGTH = 4;
  private final static int LEVEL_COUNT_LENGTH = 4;
  private final static int POINTER_SIZE = 4;

  private final static int PREFIX_OFFSET = -(PREFIX_LENGTH + SN_LENGTH + TYPE_LENGTH + KEY_LENGTH
      + VALUE_LENGTH);

  @BeforeEach
  void setUp() {
    hotArena = new Arena();
    coldArena = new Arena();
    skipList = new SkipList(hotArena,coldArena);
    skipList.init();
    testScope = java.lang.foreign.Arena.ofShared();
  }

  @AfterEach
  void tearDown() {
    hotArena.close();
    if (testScope.scope().isAlive()) {
      testScope.close();
    }
  }

  @Test
  void testPutAndGet() {
    MemorySegment key1 = createKey("12");
    MemorySegment key2 = createKey("12");
    MemorySegment val1 = createValue("value1");
    MemorySegment val2 = createValue("value1");

    skipList.insert(key1, 10, (byte) 1, val1);
    skipList.insert(key2, 13, (byte) 1, val2);

    MemorySegment tempKey = createKey("12");

    int offset1 = 0;

    for(int i = 0; i < 5_000_000; i++){
      offset1 = skipList.get(tempKey, 10);
    }


    List<Integer> offsets = new ArrayList<>();
    skipList.forEach(offsets::add);

    for(int i : offsets){
      System.out.println(fromOffsetToSN(i));
    }


    //assertEquals("key1", toString(checkHeader1.key()));
    //assertEquals("value2", toString(checkFooter1.value()));
    //assertEquals("key1", toString(checkHeader2.key()));
    //assertEquals("value2", toString(checkFooter2.value()));
  }

  @Test
  void testMultipleInsertionsAndRetrievalOrder() {
    MemorySegment kA = createKey("A"); MemorySegment vA = createValue("valA");
    MemorySegment kB = createKey("B"); MemorySegment vB = createValue("valB");
    MemorySegment kC = createKey("C"); MemorySegment vC = createValue("valC");

    skipList.insert(kB, 1L, (byte) 1, vB);
    skipList.insert(kC, 1L, (byte) 1, vC);
    skipList.insert(kA, 1L, (byte) 1, vA);

    int offsetA = skipList.get(kA, 1L);
    int offsetB = skipList.get(kB, 1L);
    int offsetC = skipList.get(kC, 1L);

    System.out.println(offsetA);


    List<Integer> offsetsFromIterator = new ArrayList<>();
    skipList.forEach(offsetsFromIterator::add);
    System.out.println(offsetsFromIterator);

    //assertEquals(3, offsetsFromIterator.size());
    //assertEquals(offsetA, offsetsFromIterator.get(0));
    //assertEquals(offsetB, offsetsFromIterator.get(1));
    //assertEquals(offsetC, offsetsFromIterator.get(2));
  }

  private MemorySegment createValue(String valueStr) {
    byte[] valueBytes = valueStr.getBytes(StandardCharsets.UTF_8);
    MemorySegment valueSegment = testScope.allocate(valueBytes.length);
    valueSegment.copyFrom(MemorySegment.ofArray(valueBytes));
    return valueSegment;
  }

  private MemorySegment createKey(String keyStr) {
    byte[] keyBytes = keyStr.getBytes(StandardCharsets.UTF_8);
    MemorySegment keySegment = testScope.allocate(keyBytes.length);
    keySegment.copyFrom(MemorySegment.ofArray(keyBytes));
    return keySegment;
  }


  private String toString(MemorySegment segment) {
    byte[] bytes = segment.toArray(ValueLayout.JAVA_BYTE);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  private long fromOffsetToSN(int offset){
    offset += COLD_ARENA_POINTER_OFFSET;
    int coldArenaPointer = hotArena.readInt(offset);
    return coldArena.readLong(coldArenaPointer);

  }

  @Test
  void testInliningWithMassiveInsertsAndGets() {
    int insertCount = 10_000;
    int getIterations = 500;

    // Pre-build all keys and values to keep the hot loop free of String allocation noise
    MemorySegment[] insertKeys = new MemorySegment[insertCount];
    long[] insertSNs = new long[insertCount];
    MemorySegment[] insertValues = new MemorySegment[insertCount];
    for (int i = 0; i < insertCount; i++) {
      String key = "key_" + String.format("%06d", i);
      insertKeys[i] = createKey(key);
      insertSNs[i] = (long) i;
      insertValues[i] = createValue("val_" + i);
    }

    // Phase 1: bulk insert — heats up insert path
    for (int i = 0; i < insertCount; i++) {
      skipList.insert(insertKeys[i], insertSNs[i], (byte) 1, insertValues[i]);
    }

    // Pre-build query keys (reuse same key/SN so get() finds them)
    MemorySegment[] queryKeys = new MemorySegment[insertCount];
    long[] querySNs = new long[insertCount];
    for (int i = 0; i < insertCount; i++) {
      String key = "key_" + String.format("%06d", i);
      queryKeys[i] = createKey(key);
      querySNs[i] = (long) i;
    }

    // Phase 2: tight hot loop on get() — this is where inlining should kick in
    // After ~10k invocations C1 compiles, after ~50-100k C2 compiles with inlining
    int found = 0;
    for (int iter = 0; iter < getIterations; iter++) {
      for (int i = 0; i < insertCount; i++) {
        int offset = skipList.get(queryKeys[i], querySNs[i]);
        if (offset != -1) {
          found++;
        }
      }
    }

    assertEquals((long) insertCount * getIterations, found,
        "All inserted keys must be found on every iteration");

    // Phase 3: miss loop — exercises the early-exit / isNull branch
    MemorySegment[] missKeys = new MemorySegment[1000];
    for (int i = 0; i < 1000; i++) {
      missKeys[i] = createKey("miss_" + String.format("%06d", i));
    }
    int misses = 0;
    for (int iter = 0; iter < getIterations; iter++) {
      for (int i = 0; i < 1000; i++) {
        if (skipList.get(missKeys[i], 1L) == -1) {
          misses++;
        }
      }
    }

    assertEquals(1000 * getIterations, misses, "All miss keys should return -1");
  }


}