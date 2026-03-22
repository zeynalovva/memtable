package az.zeynalov.tests;

import az.zeynalov.memtable.Arena;
import az.zeynalov.memtable.SkipList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

@Tag("stress")
class SkipListBoundsStressTest {

  private Arena hotArena;
  private Arena coldArena;
  private SkipList skipList;
  private java.lang.foreign.Arena testScope;

  @BeforeEach
  void setUp() {
    hotArena = new Arena();
    coldArena = new Arena();
    skipList = new SkipList(hotArena, coldArena);
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
  void heavyInsertionAndRetrieval_withMixedKeySizes_noOutOfBoundsAndCorrectReads() {
    final int total = Integer.getInteger("skiplist.stress.total", 220_000);
    final int sampleChecks = Integer.getInteger("skiplist.stress.sampleChecks", 40_000);
    final int maxKeyLen = Integer.getInteger("skiplist.stress.maxKeyLen", 96);

    MemorySegment[] insertedKeys = new MemorySegment[total];
    long[] insertedSNs = new long[total];

    for (int i = 0; i < total; i++) {
      byte[] keyBytes = buildKeyBytes(i, maxKeyLen);
      MemorySegment keySegment = testScope.allocate(keyBytes.length);
      keySegment.copyFrom(MemorySegment.ofArray(keyBytes));

      long sn = i + 1L;
      MemorySegment valueSeg = createValue("v-" + i);

      skipList.insert(keySegment, sn, (byte) 1, valueSeg);
      insertedKeys[i] = keySegment;
      insertedSNs[i] = sn;

      if ((i & 4095) == 0) {
        int off = skipList.get(keySegment, sn);
        assertNotEquals(-1, off, "inserted key must be retrievable at i=" + i);
      }
    }

    for (int i = 0; i < total; i++) {
      int off = skipList.get(insertedKeys[i], insertedSNs[i]);
      assertNotEquals(-1, off, "missing key at index=" + i);
    }

    Random rnd = new Random(123456789L);
    for (int i = 0; i < sampleChecks; i++) {
      int index = rnd.nextInt(total);
      MemorySegment key = insertedKeys[index];
      long sn = insertedSNs[index];

      int foundOffset = skipList.get(key, sn);
      assertNotEquals(-1, foundOffset, "random probe must find inserted key idx=" + index);

      int coldOffset = readColdOffsetFromHotNode(foundOffset);
      assertEquals(index + 1L, coldArena.readLong(coldOffset), "SN must match inserted SN");
      assertEquals(key.byteSize(), coldArena.readInt(coldOffset + 12), "key size mismatch");

      int valueSize = coldArena.readInt(coldOffset + 16);
      MemorySegment value = coldArena.readBytes(coldOffset + 20 + (int) key.byteSize(), valueSize);
      String actualValue = new String(value.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
      assertEquals("v-" + index, actualValue, "value bytes mismatch");
    }
  }

  private MemorySegment createValue(String valueStr) {
    byte[] valueBytes = valueStr.getBytes(StandardCharsets.UTF_8);
    MemorySegment valueSegment = testScope.allocate(valueBytes.length);
    valueSegment.copyFrom(MemorySegment.ofArray(valueBytes));
    return valueSegment;
  }

  private int readColdOffsetFromHotNode(int nodeOffset) {
    return hotArena.readInt(nodeOffset + 12);
  }

  private byte[] buildKeyBytes(int index, int maxKeyLen) {
    int lenSwitch = index % 10;
    int len = switch (lenSwitch) {
      case 0 -> 1;
      case 1 -> 2;
      case 2 -> 7;
      case 3 -> 8;
      case 4 -> 15;
      case 5 -> 31;
      case 6 -> 32;
      case 7 -> 33;
      case 8 -> 64;
      default -> maxKeyLen;
    };

    byte[] bytes = new byte[len];
    long seed = 0x9E3779B97F4A7C15L ^ (long) index;
    for (int i = 0; i < len; i++) {
      seed ^= (seed << 13);
      seed ^= (seed >>> 7);
      seed ^= (seed << 17);
      bytes[i] = (byte) (seed & 0xFF);
    }
    return bytes;
  }
}

