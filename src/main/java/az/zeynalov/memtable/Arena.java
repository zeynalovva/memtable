package az.zeynalov.memtable;

import az.zeynalov.memtable.exception.ArenaCapacityException;
import az.zeynalov.memtable.exception.ErrorMessage;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Allocated memory (64mb) using Foreign Memory API (MemorySegment)
 */
public class Arena implements AutoCloseable {

  private final static long ALLOCATED_MEMORY_SIZE = 64L * (1 << 20);

  private final static ValueLayout.OfLong BE_LONG = ValueLayout.JAVA_LONG.withOrder(
      ByteOrder.BIG_ENDIAN);
  private final static ValueLayout.OfInt BE_INT = ValueLayout.JAVA_INT.withOrder(
      ByteOrder.BIG_ENDIAN);

  private final AtomicInteger availableOffset;
  private final java.lang.foreign.Arena offHeapScope;

  public final MemorySegment memory;

  public Arena() {
    this.offHeapScope = java.lang.foreign.Arena.ofShared();
    this.memory = offHeapScope.allocate(ALLOCATED_MEMORY_SIZE);
    this.availableOffset = new AtomicInteger(0);
  }

  public int allocate(int sizeOfPayload) {
    int current, alignedOffset, next;
    do {
      current = availableOffset.get();
      alignedOffset = (current + 7) & ~7;
      next = alignedOffset + sizeOfPayload;

      if (next > ALLOCATED_MEMORY_SIZE) {
        throw ArenaCapacityException.of(ErrorMessage.ARENA_IS_FULL);
      }
    } while (!availableOffset.compareAndSet(current, next));

    return alignedOffset;
  }

  public MemorySegment getMemory() {
    return memory;
  }

  public int getArenaSize() {
    return availableOffset.get();
  }

  public MemorySegment readBytes(int offset, int length) {
    return memory.asSlice(offset, length);
  }

  public int readInt(int offset) {
    return memory.get(BE_INT, offset);
  }

  public long readLong(int offset) {
    return memory.get(BE_LONG, offset);
  }

  public byte readByte(int offset) {
    return memory.get(ValueLayout.JAVA_BYTE, offset);
  }

  public void writeBytes(int offset, MemorySegment payload) {
    MemorySegment.copy(payload, 0, this.memory, offset, payload.byteSize());
  }

  public void writeByte(int offset, byte payload) {
    memory.set(ValueLayout.JAVA_BYTE, offset, payload);
  }

  public void writeLong(int offset, long payload) {
    memory.set(BE_LONG, offset, payload);
  }

  public void writeInt(int offset, int payload) {
    memory.set(BE_INT, offset, payload);
  }

  @Override
  public void close() {
    if (offHeapScope.scope().isAlive()) {
      offHeapScope.close();
    }
  }

}