package az.zeynalov.tests;

import static org.junit.jupiter.api.Assertions.*;

import az.zeynalov.memtable.Arena;
import az.zeynalov.memtable.exception.ArenaCapacityException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class ArenaTest {

  private Arena arena;

  @BeforeEach
  public void setup() {
    arena = new Arena();
  }

  @AfterEach
  public void tearDown() {
    arena.close();
  }

  @Nested
  class Allocation {

    @Test
    void firstAllocationReturnsZeroOffset() {
      int offset = arena.allocate(64);
      assertEquals(0, offset);
    }

    @Test
    void allocationAdvancesArenaSize() {
      arena.allocate(100);
      assertEquals(100, arena.getArenaSize());
    }

    @Test
    void consecutiveAllocationsReturnIncrementingOffsets() {
      int first = arena.allocate(50);
      int second = arena.allocate(30);
      int third = arena.allocate(20);

      assertEquals(0, first);
      assertEquals(50, second);
      assertEquals(80, third);
      assertEquals(100, arena.getArenaSize());
    }

    @Test
    void allocationExceedingCapacityThrows() {
      int oversized = 65 * (1 << 20);
      assertThrows(ArenaCapacityException.class, () -> arena.allocate(oversized));
    }

    @Test
    void allocationExactlyAtCapacitySucceeds() {
      int capacity = 64 * (1 << 20);
      assertDoesNotThrow(() -> arena.allocate(capacity));
      assertEquals(capacity, arena.getArenaSize());
    }

    @Test
    void allocationOneByteOverCapacityThrows() {
      int capacity = 64 * (1 << 20);
      arena.allocate(capacity);
      assertThrows(ArenaCapacityException.class, () -> arena.allocate(1));
    }

    @Test
    void sizeIsZeroBeforeAnyAllocation() {
      assertEquals(0, arena.getArenaSize());
    }

    @Test
    void multipleSmallAllocationsEventuallyExceedCapacity() {
      int chunkSize = 32 * (1 << 20);
      arena.allocate(chunkSize);
      arena.allocate(chunkSize);
      assertThrows(ArenaCapacityException.class, () -> arena.allocate(1));
    }
  }

  @Nested
  class IntReadWrite {

    @Test
    void writeAndReadIntAtOffsetZero() {
      arena.allocate(Integer.BYTES);
      arena.writeInt(0, 42);
      assertEquals(42, arena.readInt(0));
    }

    @Test
    void writeAndReadNegativeInt() {
      arena.allocate(Integer.BYTES);
      arena.writeInt(0, -1);
      assertEquals(-1, arena.readInt(0));
    }

    @Test
    void writeAndReadIntMaxValue() {
      arena.allocate(Integer.BYTES);
      arena.writeInt(0, Integer.MAX_VALUE);
      assertEquals(Integer.MAX_VALUE, arena.readInt(0));
    }

    @Test
    void writeAndReadIntMinValue() {
      arena.allocate(Integer.BYTES);
      arena.writeInt(0, Integer.MIN_VALUE);
      assertEquals(Integer.MIN_VALUE, arena.readInt(0));
    }

    @Test
    void writeMultipleIntsAtDifferentOffsets() {
      arena.allocate(Integer.BYTES * 3);
      arena.writeInt(0, 10);
      arena.writeInt(Integer.BYTES, 20);
      arena.writeInt(Integer.BYTES * 2, 30);

      assertEquals(10, arena.readInt(0));
      assertEquals(20, arena.readInt(Integer.BYTES));
      assertEquals(30, arena.readInt(Integer.BYTES * 2));
    }

    @Test
    void writeZeroInt() {
      arena.allocate(Integer.BYTES);
      arena.writeInt(0, 0);
      assertEquals(0, arena.readInt(0));
    }

    @Test
    void overwriteIntAtSameOffset() {
      arena.allocate(Integer.BYTES);
      arena.writeInt(0, 100);
      arena.writeInt(0, 200);
      assertEquals(200, arena.readInt(0));
    }
  }

  @Nested
  class LongReadWrite {

    @Test
    void writeAndReadLong() {
      arena.allocate(Long.BYTES);
      arena.writeLong(0, 123456789L);
      assertEquals(123456789L, arena.readLong(0));
    }

    @Test
    void writeAndReadNegativeLong() {
      arena.allocate(Long.BYTES);
      arena.writeLong(0, -99999L);
      assertEquals(-99999L, arena.readLong(0));
    }

    @Test
    void writeAndReadLongMaxValue() {
      arena.allocate(Long.BYTES);
      arena.writeLong(0, Long.MAX_VALUE);
      assertEquals(Long.MAX_VALUE, arena.readLong(0));
    }

    @Test
    void writeAndReadLongMinValue() {
      arena.allocate(Long.BYTES);
      arena.writeLong(0, Long.MIN_VALUE);
      assertEquals(Long.MIN_VALUE, arena.readLong(0));
    }

    @Test
    void writeZeroLong() {
      arena.allocate(Long.BYTES);
      arena.writeLong(0, 0L);
      assertEquals(0L, arena.readLong(0));
    }

    @Test
    void overwriteLongAtSameOffset() {
      arena.allocate(Long.BYTES);
      arena.writeLong(0, 111L);
      arena.writeLong(0, 222L);
      assertEquals(222L, arena.readLong(0));
    }
  }

  @Nested
  class ByteReadWrite {

    @Test
    void writeAndReadByte() {
      arena.allocate(Byte.BYTES);
      arena.writeByte(0, (byte) 0x7F);
      assertEquals((byte) 0x7F, arena.readByte(0));
    }

    @Test
    void writeAndReadZeroByte() {
      arena.allocate(Byte.BYTES);
      arena.writeByte(0, (byte) 0);
      assertEquals((byte) 0, arena.readByte(0));
    }

    @Test
    void writeAndReadByteMaxValue() {
      arena.allocate(Byte.BYTES);
      arena.writeByte(0, Byte.MAX_VALUE);
      assertEquals(Byte.MAX_VALUE, arena.readByte(0));
    }

    @Test
    void writeAndReadByteMinValue() {
      arena.allocate(Byte.BYTES);
      arena.writeByte(0, Byte.MIN_VALUE);
      assertEquals(Byte.MIN_VALUE, arena.readByte(0));
    }

    @Test
    void writeAllByteValues() {
      arena.allocate(256);
      for (int i = 0; i < 256; i++) {
        arena.writeByte(i, (byte) i);
      }
      for (int i = 0; i < 256; i++) {
        assertEquals((byte) i, arena.readByte(i));
      }
    }
  }

  @Nested
  class BytesReadWrite {

    @Test
    void writeAndReadByteArray() {
      byte[] data = {1, 2, 3, 4, 5};
      MemorySegment payload = MemorySegment.ofArray(data);
      arena.allocate(data.length);
      arena.writeBytes(0, payload);

      MemorySegment result = arena.readBytes(0, data.length);
      for (int i = 0; i < data.length; i++) {
        assertEquals(data[i], result.get(ValueLayout.JAVA_BYTE, i));
      }
    }

    @Test
    void writeAndReadEmptyByteArray() {
      byte[] data = {};
      MemorySegment payload = MemorySegment.ofArray(data);
      arena.allocate(1);
      assertDoesNotThrow(() -> arena.writeBytes(0, payload));
    }

    @Test
    void writeAndReadBytesAtNonZeroOffset() {
      byte[] prefix = {10, 20};
      byte[] data = {30, 40, 50};
      arena.allocate(prefix.length + data.length);
      arena.writeBytes(0, MemorySegment.ofArray(prefix));
      arena.writeBytes(prefix.length, MemorySegment.ofArray(data));

      MemorySegment result = arena.readBytes(prefix.length, data.length);
      assertEquals(30, result.get(ValueLayout.JAVA_BYTE, 0));
      assertEquals(40, result.get(ValueLayout.JAVA_BYTE, 1));
      assertEquals(50, result.get(ValueLayout.JAVA_BYTE, 2));
    }

    @Test
    void readBytesReturnsCorrectSliceLength() {
      byte[] data = {1, 2, 3, 4, 5, 6, 7, 8};
      arena.allocate(data.length);
      arena.writeBytes(0, MemorySegment.ofArray(data));

      MemorySegment slice = arena.readBytes(2, 3);
      assertEquals(3, slice.byteSize());
      assertEquals(3, slice.get(ValueLayout.JAVA_BYTE, 0));
      assertEquals(4, slice.get(ValueLayout.JAVA_BYTE, 1));
      assertEquals(5, slice.get(ValueLayout.JAVA_BYTE, 2));
    }

    @Test
    void writeLargeByteArray() {
      int size = 1024;
      byte[] data = new byte[size];
      for (int i = 0; i < size; i++) {
        data[i] = (byte) (i % 256);
      }
      arena.allocate(size);
      arena.writeBytes(0, MemorySegment.ofArray(data));

      MemorySegment result = arena.readBytes(0, size);
      for (int i = 0; i < size; i++) {
        assertEquals(data[i], result.get(ValueLayout.JAVA_BYTE, i));
      }
    }
  }

  @Nested
  class VarintReadWrite {

    @Test
    void singleByteVarintZero() {
      arena.allocate(10);
      arena.writeVarint(0, 0);
      long packed = arena.readVarint(0);
      assertEquals(0, unpackValue(packed));
      assertEquals(1, unpackBytesRead(packed));
    }

    @Test
    void singleByteVarintOne() {
      arena.allocate(10);
      arena.writeVarint(0, 1);
      long packed = arena.readVarint(0);
      assertEquals(1, unpackValue(packed));
      assertEquals(1, unpackBytesRead(packed));
    }

    @Test
    void singleByteMaxVarint() {
      arena.allocate(10);
      arena.writeVarint(0, 127);
      long packed = arena.readVarint(0);
      assertEquals(127, unpackValue(packed));
      assertEquals(1, unpackBytesRead(packed));
    }

    @Test
    void twoByteVarintBoundary() {
      arena.allocate(10);
      arena.writeVarint(0, 128);
      long packed = arena.readVarint(0);
      assertEquals(128, unpackValue(packed));
      assertEquals(2, unpackBytesRead(packed));
    }

    @Test
    void twoByteVarintMaxValue() {
      arena.allocate(10);
      int value = (1 << 14) - 1; // 16383
      arena.writeVarint(0, value);
      long packed = arena.readVarint(0);
      assertEquals(value, unpackValue(packed));
      assertEquals(2, unpackBytesRead(packed));
    }

    @Test
    void threeByteVarintBoundary() {
      arena.allocate(10);
      int value = 1 << 14; // 16384
      arena.writeVarint(0, value);
      long packed = arena.readVarint(0);
      assertEquals(value, unpackValue(packed));
      assertEquals(3, unpackBytesRead(packed));
    }

    @Test
    void fourByteVarint() {
      arena.allocate(10);
      int value = 1 << 21; // 2097152
      arena.writeVarint(0, value);
      long packed = arena.readVarint(0);
      assertEquals(value, unpackValue(packed));
      assertEquals(4, unpackBytesRead(packed));
    }

    @Test
    void fiveByteVarintMaxInt() {
      arena.allocate(10);
      arena.writeVarint(0, Integer.MAX_VALUE);
      long packed = arena.readVarint(0);
      assertEquals(Integer.MAX_VALUE, unpackValue(packed));
      assertEquals(5, unpackBytesRead(packed));
    }

    @Test
    void multipleVarintsAtDifferentOffsets() {
      arena.allocate(20);
      arena.writeVarint(0, 1);
      arena.writeVarint(5, 500);
      arena.writeVarint(10, 100000);

      assertEquals(1, unpackValue(arena.readVarint(0)));
      assertEquals(500, unpackValue(arena.readVarint(5)));
      assertEquals(100000, unpackValue(arena.readVarint(10)));
    }

    @Test
    void varintRoundTripForPowersOfTwo() {
      arena.allocate(50);
      int offset = 0;
      for (int power = 0; power < 30; power++) {
        int value = 1 << power;
        arena.writeVarint(offset, value);
        long packed = arena.readVarint(offset);
        assertEquals(value, unpackValue(packed),
            "Failed round-trip for 2^" + power);
        offset += 5; // skip enough bytes for next varint
      }
    }

    private int unpackValue(long packed) {
      return (int) (packed >> 32);
    }

    private int unpackBytesRead(long packed) {
      return (int) packed;
    }
  }

  @Nested
  class MixedTypeReadWrite {

    @Test
    void writeIntThenLongAtSubsequentOffsets() {
      arena.allocate(Integer.BYTES + Long.BYTES);
      arena.writeInt(0, 42);
      arena.writeLong(Integer.BYTES, 99L);

      assertEquals(42, arena.readInt(0));
      assertEquals(99L, arena.readLong(Integer.BYTES));
    }

    @Test
    void writeByteThenIntThenLong() {
      int totalSize = Byte.BYTES + Integer.BYTES + Long.BYTES;
      arena.allocate(totalSize);

      arena.writeByte(0, (byte) 0xFF);
      arena.writeInt(Byte.BYTES, 256);
      arena.writeLong(Byte.BYTES + Integer.BYTES, Long.MAX_VALUE);

      assertEquals((byte) 0xFF, arena.readByte(0));
      assertEquals(256, arena.readInt(Byte.BYTES));
      assertEquals(Long.MAX_VALUE, arena.readLong(Byte.BYTES + Integer.BYTES));
    }

    @Test
    void writeVarintFollowedByBytes() {
      arena.allocate(20);
      byte[] data = {10, 20, 30};
      arena.writeVarint(0, data.length);
      arena.writeBytes(5, MemorySegment.ofArray(data));

      long packed = arena.readVarint(0);
      int length = (int) (packed >> 32);
      assertEquals(3, length);

      MemorySegment result = arena.readBytes(5, length);
      assertEquals(10, result.get(ValueLayout.JAVA_BYTE, 0));
      assertEquals(20, result.get(ValueLayout.JAVA_BYTE, 1));
      assertEquals(30, result.get(ValueLayout.JAVA_BYTE, 2));
    }
  }

  @Nested
  class MemorySegmentAccess {

    @Test
    void getMemoryReturnsNonNull() {
      assertNotNull(arena.getMemory());
    }

    @Test
    void getMemoryReturnsConsistentReference() {
      MemorySegment first = arena.getMemory();
      MemorySegment second = arena.getMemory();
      assertSame(first, second);
    }

    @Test
    void memorySegmentHasExpectedSize() {
      MemorySegment mem = arena.getMemory();
      assertEquals(64L * (1 << 20), mem.byteSize());
    }
  }

  @Nested
  class Lifecycle {

    @Test
    void freshArenaHasZeroSize() {
      assertEquals(0, arena.getArenaSize());
    }

    @Test
    void closeReleasesMemory() {
      arena.close();
      assertFalse(arena.getMemory().scope().isAlive());
    }

    @Test
    void closeCanBeCalledMultipleTimes() {
      arena.close();
      assertDoesNotThrow(() -> arena.close());
    }
  }
}
