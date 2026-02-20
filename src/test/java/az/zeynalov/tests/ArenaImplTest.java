package az.zeynalov.tests;

import static org.junit.jupiter.api.Assertions.*;

import az.zeynalov.memtable.ArenaImpl;
import az.zeynalov.memtable.Pair;
import az.zeynalov.memtable.exception.ArenaCapacityException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ArenaImplTest {

  private ArenaImpl arena;

  @BeforeEach
  public void setup() {
    this.arena = new ArenaImpl();
  }

  @AfterEach
  public void tearDown() {
    arena.close();
  }

  @Test
  public void allocate_success_returnsNewOffset() {
    int offset = arena.allocate(100);
    assertEquals(0, offset);
    assertEquals(100, arena.getArenaSize());
  }

  @Test
  public void allocate_multipleTimes_incrementsOffset() {
    arena.allocate(50);
    int offset = arena.allocate(30);
    assertEquals(50, offset);
  }

  @Test
  public void allocate_exceedsCapacity_throwsException() {
    int extraSize = 65 * (1 << 20);
    assertThrows(ArenaCapacityException.class, () -> arena.allocate(extraSize));
  }

  @Test
  public void writeInt_andReadInt_success() {
    arena.allocate(Integer.BYTES);
    arena.writeInt(0, 42);
    int result = arena.readInt(0);
    assertEquals(42, result);
  }

  @Test
  public void readInt_offsetExceedsSize_throwsException() {
    assertThrows(ArenaCapacityException.class, () -> arena.readInt(0));
  }

  @Test
  public void writeBytes_andReadBytes_success() {
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
  public void readBytes_exceedsSize_throwsException() {
    arena.allocate(5);
    assertThrows(ArenaCapacityException.class, () -> arena.readBytes(0, 10));
  }

  @Test
  public void writeBytes_offsetExceedsSize_throwsException() {
    MemorySegment payload = MemorySegment.ofArray(new byte[]{1, 2, 3});
    assertThrows(ArenaCapacityException.class, () -> arena.writeBytes(100, payload));
  }

  @Test
  public void writeVarint_andReadVarint_smallValue() {
    arena.allocate(10);
    arena.writeVarint(127, 0);
    Pair<Integer, Integer> result = arena.readVarint(0);
    assertEquals(127, result.value());
    assertEquals(1, result.numberOfBytes());
  }

  @Test
  public void writeVarint_andReadVarint_largeValue() {
    arena.allocate(10);
    int value = 300;
    arena.writeVarint(value, 0);
    Pair<Integer, Integer> result = arena.readVarint(0);
    assertEquals(value, result.value());
    assertTrue(result.numberOfBytes() > 1);
  }

  @Test
  public void getArenaSize_initiallyZero() {
    assertEquals(0, arena.getArenaSize());
  }

  @Test
  public void writeLong_andReadLong_success() {
    arena.allocate(Long.BYTES);
    arena.writeLong(0, 123456789L);
    long result = arena.readLong(0);
    assertEquals(123456789L, result);
  }

  @Test
  public void readLong_offsetExceedsSize_throwsException() {
    assertThrows(ArenaCapacityException.class, () -> arena.readLong(0));
  }

  @Test
  public void writeLong_offsetExceedsSize_throwsException() {
    assertThrows(ArenaCapacityException.class, () -> arena.writeLong(100, 42L));
  }

  @Test
  public void writeByte_andReadByte_success() {
    arena.allocate(Byte.BYTES);
    arena.writeByte(0, (byte) 0x7F);
    byte result = arena.readByte(0);
    assertEquals((byte) 0x7F, result);
  }

  @Test
  public void readByte_offsetExceedsSize_throwsException() {
    assertThrows(ArenaCapacityException.class, () -> arena.readByte(0));
  }

  @Test
  public void writeByte_offsetExceedsSize_throwsException() {
    assertThrows(ArenaCapacityException.class, () -> arena.writeByte(100, (byte) 1));
  }

  @Test
  public void close_canBeCalledMultipleTimes() {
    arena.close();
    assertDoesNotThrow(() -> arena.close());
  }
}

