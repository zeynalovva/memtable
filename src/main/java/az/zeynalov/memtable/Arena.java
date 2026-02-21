package az.zeynalov.memtable;

import az.zeynalov.memtable.exception.ArenaCapacityException;
import az.zeynalov.memtable.exception.ErrorMessage;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Allocated memory (64mb) using Foreign Memory API (MemorySegment)
 */
public class Arena implements AutoCloseable {

  private final static long ALLOCATED_MEMORY_SIZE = 64L * (1 << 20);
  private static final ValueLayout.OfInt UNALIGNED_INT =
      ValueLayout.JAVA_INT.withByteAlignment(1);
  private static final ValueLayout.OfLong UNALIGNED_LONG =
      ValueLayout.JAVA_LONG.withByteAlignment(1);
  private final AtomicInteger availableOffset;
  private final java.lang.foreign.Arena offHeapScope;

  public final MemorySegment memory;

  public Arena() {
    this.offHeapScope = java.lang.foreign.Arena.ofShared();
    this.memory = offHeapScope.allocate(ALLOCATED_MEMORY_SIZE);
    this.availableOffset = new AtomicInteger(0);
  }

  public int allocate(int sizeOfPayload) {
    int offset = availableOffset.get();
    if (offset + sizeOfPayload > ALLOCATED_MEMORY_SIZE) {
      throw ArenaCapacityException.of(ErrorMessage.ARENA_IS_FULL);
    }
    availableOffset.set(offset + sizeOfPayload);
    return offset;
  }

  public long readVarint(int offset) {
    int value = 0;
    int shift = 0;
    int currentOffset = offset;
    int bytesRead = 0;

    while (true) {
      if (currentOffset >= ALLOCATED_MEMORY_SIZE) {
        throw new RuntimeException("Buffer underflow");
      }

      byte b = memory.get(ValueLayout.JAVA_BYTE, currentOffset);
      currentOffset++;
      bytesRead++;

      value |= (b & 0x7F) << shift;

      if ((b & 0x80) == 0) {
        return pack(value, bytesRead);
      }

      shift += 7;

      if (shift >= 35) {
        throw new RuntimeException("Varint is too large (overflow)"); // TODO make errors static
      }
    }
  }

  public void writeVarint(int offset, int value) {
    int currentOffset = offset;

    while (true) {
      if ((value & ~0x7F) == 0) {
        memory.set(ValueLayout.JAVA_BYTE, currentOffset, (byte) value);
        return;
      } else {
        memory.set(ValueLayout.JAVA_BYTE, currentOffset, (byte) ((value & 0x7F) | 0x80));
        value >>>= 7;
        currentOffset++;
      }
    }
  }

  public MemorySegment getMemory() {
    return memory;
  }

  public int getArenaSize(){
    return availableOffset.get();
  }

  public MemorySegment readBytes(int offset, int length) {
    return memory.asSlice(offset, length);
  }

  public int readInt(int offset) {
    return memory.get(UNALIGNED_INT, offset);
  }

  public long readLong(int offset){
    return memory.get(UNALIGNED_LONG, offset);
  }

  public byte readByte(int offset){
    return memory.get(ValueLayout.JAVA_BYTE, offset);
  }

  public void writeBytes(int offset, MemorySegment payload) {
    MemorySegment.copy(payload, 0, this.memory, offset, payload.byteSize());
  }

  public void writeByte(int offset, byte payload){
    memory.set(ValueLayout.JAVA_BYTE, offset, payload);
  }

  public void writeLong(int offset, long payload){
    memory.set(UNALIGNED_LONG, offset, payload);
  }

  public void writeInt(int offset, int payload) {
    memory.set(UNALIGNED_INT, offset, payload);
  }

  @Override
  public void close() {
    if (offHeapScope.scope().isAlive()) {
      offHeapScope.close();
    }
  }

  private long pack(int a, int b) {
    return ((long) a << 32) | (b & 0xFFFFFFFFL);
  }
}