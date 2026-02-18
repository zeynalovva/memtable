package az.zeynalov.memtable;

import az.zeynalov.memtable.exception.ArenaCapacityException;
import az.zeynalov.memtable.exception.ErrorMessage;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This will allocate memory (64mb) and return the first free offset
 */
public class Arena {
  /// TODO check offsets
  private final static int ALLOCATED_MEMORY_SIZE = 64 * (1 << 20);

  private final AtomicInteger availableOffset;
  public final ByteBuffer memory;

  public Arena() {
    this.memory = ByteBuffer.allocateDirect(ALLOCATED_MEMORY_SIZE);
    this.availableOffset = new AtomicInteger(0);
  }

  public int allocate(int sizeOfPayload) {
    int offset = availableOffset.getAndAdd(sizeOfPayload);
    if (offset + sizeOfPayload >= ALLOCATED_MEMORY_SIZE) {
      throw ArenaCapacityException.of(ErrorMessage.ARENA_IS_FULL);
    }

    return offset;
  }

  public Pair<Integer, Integer> readVarint(int offset) {
    memory.position(offset);
    int value = 0;
    int shift = 0;
    int bytesRead = 0;

    while (true) {
      if (!memory.hasRemaining()) {
        throw new RuntimeException("Buffer underflow");
      }

      byte b = memory.get();
      bytesRead++;


      value |= (b & 0x7F) << shift;

      if ((b & 0x80) == 0) {

        return new Pair<>(value, bytesRead);
      }

      shift += 7;

      if (shift >= 35) {
        throw new RuntimeException("Varint is too large (overflow)");
      }
    }
  }

  public void writeVarint(int value, int offset) {
    memory.position(offset);
    while (true) {
      if ((value & ~0x7F) == 0) {
        memory.put((byte) value);
        return;
      } else {
        memory.put((byte) ((value & 0x7F) | 0x80));
        value >>>= 7;
      }
    }
  }


  public int getVarintSize(int value) {
    if ((value & (0xFFFFFFFF << 7)) == 0) return 1;
    if ((value & (0xFFFFFFFF << 14)) == 0) return 2;
    if ((value & (0xFFFFFFFF << 21)) == 0) return 3;
    if ((value & (0xFFFFFFFF << 28)) == 0) return 4;
    return 5;
  }

  public int currentSize(){
    return availableOffset.get();
  }


  public ByteBuffer read(int offset, int length){
    return memory.slice(offset, length);
  }

  public int readInt(int offset){
      return memory.getInt(offset);
  }

  public void write(int offset, ByteBuffer payload){
    if (offset > availableOffset.get()) {
      throw ArenaCapacityException.of(ErrorMessage.ARENA_LIMIT_EXCEED);
    }
    memory.position(offset);
    memory.put(payload);
  }

  public void writeInt(int offset, int payload){
    if (offset > availableOffset.get()) {
      throw ArenaCapacityException.of(ErrorMessage.ARENA_LIMIT_EXCEED);
    }
    memory.putInt(offset, payload);
  }
}
