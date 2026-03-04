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

  private Arena hotArena;
  private Arena coldArena;
  private SkipList skipList;
  private java.lang.foreign.Arena testScope;

  private final static int PREFIX_LENGTH = 8;
  private final static int SN_LENGTH = 8;
  private final static int TYPE_LENGTH = 4;
  private final static int KEY_LENGTH = 4;
  private final static int VALUE_LENGTH = 4;

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

  void testPutAndGet() {
    Header header1 = createHeader("12", 10, (byte) 1);
    Header header2 = createHeader("12", 13, (byte) 1);

    Footer footer1 = createFooter("value1");
    Footer footer2 = createFooter("value1");

    skipList.insert(header1, footer1);
    skipList.insert(header2, footer2);

    Header temp = createHeader("12", 10, (byte) 1);

    int offset1 = 0;

    for(int i = 0; i < 5_000_000; i++){
      offset1 = skipList.get(temp);
    }

    Record check1 = fromOffset(offset1);


    Header checkHeader1 = check1.header();
    Footer checkFooter1 = check1.footer();

    //System.out.println("Check 1: " + toString(checkHeader1.key()) + " -> " + toString(checkFooter1.value()) + " (SN: " + checkHeader1.SN() + ")");

    //assertEquals("key1", toString(checkHeader1.key()));
    //assertEquals("value2", toString(checkFooter1.value()));
    //assertEquals("key1", toString(checkHeader2.key()));
    //assertEquals("value2", toString(checkFooter2.value()));
  }

  @Test
  void testMultipleInsertionsAndRetrievalOrder() {
    Header hA = createHeader("A", 1L, (byte) 1); Footer fA = createFooter("valA");
    Header hB = createHeader("B", 1L, (byte) 1); Footer fB = createFooter("valB");
    Header hC = createHeader("C", 1L, (byte) 1); Footer fC = createFooter("valC");

    skipList.insert(hB, fB);
    skipList.insert(hC, fC);
    skipList.insert(hA, fA);

    int offsetA = skipList.get(hA);
    int offsetB = skipList.get(hB);
    int offsetC = skipList.get(hC);

    System.out.println(offsetA);


    List<Integer> offsetsFromIterator = new ArrayList<>();
    skipList.forEach(offsetsFromIterator::add);
    System.out.println(offsetsFromIterator);

    assertEquals(3, offsetsFromIterator.size());
    assertEquals(offsetA, offsetsFromIterator.get(0));
    assertEquals(offsetB, offsetsFromIterator.get(1));
    assertEquals(offsetC, offsetsFromIterator.get(2));
  }

  private Footer createFooter(String valueStr) {
    byte[] valueBytes = valueStr.getBytes(StandardCharsets.UTF_8);
    MemorySegment valueSegment = testScope.allocate(valueBytes.length);
    valueSegment.copyFrom(MemorySegment.ofArray(valueBytes));
    return new Footer(valueBytes.length, valueSegment);
  }

  private Header createHeader(String keyStr, long SN, byte type) {
    byte[] keyBytes = keyStr.getBytes(StandardCharsets.UTF_8);
    MemorySegment keySegment = testScope.allocate(keyBytes.length);
    keySegment.copyFrom(MemorySegment.ofArray(keyBytes));
    return new Header(keyBytes.length, keySegment, SN, type);
  }


  private String toString(MemorySegment segment) {
    byte[] bytes = segment.toArray(ValueLayout.JAVA_BYTE);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  private Record fromOffset(int offset){
    offset += PREFIX_OFFSET;
    long prefix = hotArena.readLong(offset);
    offset+= 8;
    long SN = hotArena.readLong(offset);
    offset += 8;
    byte type = hotArena.readByte(offset);
    offset += 4;
    int keySize = hotArena.readInt(offset);
    offset += 4;
    int valueSize = hotArena.readInt(offset);
    offset += 4;
    int levelCount = hotArena.readInt(offset);
    int keyOffset = offset + 4 + 4 * levelCount;
    MemorySegment key = hotArena.readBytes(keyOffset, keySize);
    MemorySegment value = hotArena.readBytes(keyOffset + keySize, valueSize);
    Header temp = new Header(keySize, key, SN, type);
    Footer footer = new Footer(valueSize, value);
    return new Record(temp, footer);
  }

  @Test
  void testInliningWithMassiveInsertsAndGets() {
    int insertCount = 10_000;
    int getIterations = 500;

    // Pre-build all keys and headers to keep the hot loop free of String allocation noise
    Header[] insertHeaders = new Header[insertCount];
    Footer[] insertFooters = new Footer[insertCount];
    for (int i = 0; i < insertCount; i++) {
      String key = "key_" + String.format("%06d", i);
      insertHeaders[i] = createHeader(key, (long) i, (byte) 1);
      insertFooters[i] = createFooter("val_" + i);
    }

    // Phase 1: bulk insert — heats up insert path
    for (int i = 0; i < insertCount; i++) {
      skipList.insert(insertHeaders[i], insertFooters[i]);
    }

    // Pre-build query headers (reuse same key/SN so get() finds them)
    Header[] queryHeaders = new Header[insertCount];
    for (int i = 0; i < insertCount; i++) {
      String key = "key_" + String.format("%06d", i);
      queryHeaders[i] = createHeader(key, (long) i, (byte) 1);
    }

    // Phase 2: tight hot loop on get() — this is where inlining should kick in
    // After ~10k invocations C1 compiles, after ~50-100k C2 compiles with inlining
    int found = 0;
    for (int iter = 0; iter < getIterations; iter++) {
      for (int i = 0; i < insertCount; i++) {
        int offset = skipList.get(queryHeaders[i]);
        if (offset != -1) {
          found++;
        }
      }
    }

    assertEquals((long) insertCount * getIterations, found,
        "All inserted keys must be found on every iteration");

    // Phase 3: miss loop — exercises the early-exit / isNull branch
    Header[] missHeaders = new Header[1000];
    for (int i = 0; i < 1000; i++) {
      missHeaders[i] = createHeader("miss_" + String.format("%06d", i), 1L, (byte) 1);
    }
    int misses = 0;
    for (int iter = 0; iter < getIterations; iter++) {
      for (int i = 0; i < 1000; i++) {
        if (skipList.get(missHeaders[i]) == -1) {
          misses++;
        }
      }
    }

    assertEquals(1000 * getIterations, misses, "All miss keys should return -1");
  }

  private record Record(Header header, Footer footer) {

  }

}