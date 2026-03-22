package az.zeynalov.memtable;


import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

// TODO add concurrency for closing memtable and arenas
public class MemTable {

  private final Arena hotArena;
  private final Arena coldArena;
  private final SkipList skipList;

  public MemTable(Arena hotArena, Arena coldArena, SkipList skipList) {
    this.hotArena = hotArena;
    this.coldArena = coldArena;
    this.skipList = skipList;
  }

  public void put(MemorySegment key, long SN, byte type, MemorySegment value) {
    skipList.insert(key, SN, type, value);
  }

  public byte[] get(MemTableIterator iterator) {
    if (!iterator.isValid()) {
      return null;
    }

    int coldArenaOffset = hotArena.readInt(iterator.getCurrent() + SkipList.COLD_ARENA_POINTER_OFFSET);
    int keySizeOffset = coldArenaOffset + SkipList.KEY_SIZE_OFFSET;
    int keySize = coldArena.readInt(keySizeOffset);
    int valueSize = coldArena.readInt(keySizeOffset + SkipList.KEY_LENGTH);
    final int totalSize = SkipList.KEY_LENGTH + SkipList.VALUE_LENGTH + keySize + valueSize;

    return coldArena.readBytes(keySizeOffset, totalSize).toArray(ValueLayout.JAVA_BYTE);
  }
}
