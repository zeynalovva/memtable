package az.zeynalov.memtable;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

public interface Arena {
  int allocate(int size);
  Pair<Integer, Integer> readVarint(int offset);
  void writeVarint(int offset, int value);
  MemorySegment readBytes(int offset, int length);
  int readInt(int offset);
  long readLong(int offset);
  byte readByte(int offset);
  void writeInt(int offset, int payload);
  void writeBytes(int offset, MemorySegment payload);
  int getArenaSize();
  void writeByte(int headerOffset, byte type);
  void writeLong(int headerOffset, long sn);
}
