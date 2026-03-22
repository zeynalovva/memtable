package az.zeynalov.memtable;

import static az.zeynalov.memtable.SkipList.COLD_ARENA_POINTER_OFFSET;
import static az.zeynalov.memtable.SkipList.KEY_OFFSET;
import static az.zeynalov.memtable.SkipList.KEY_SIZE_OFFSET;
import static az.zeynalov.memtable.SkipList.VALUE_SIZE_OFFSET;

import java.lang.foreign.MemorySegment;

public class MemTable {

  private final Arena hotArena;
  private final Arena coldArena;
  private final SkipList skipList;

  public MemTable(Arena hotArena, Arena coldArena, SkipList skipList) {
    this.hotArena = hotArena;
    this.coldArena = coldArena;
    this.skipList = skipList;
  }

  public void put(MemorySegment key, MemorySegment value) {

  }
}
