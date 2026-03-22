package az.zeynalov.memtable;

import java.lang.foreign.MemorySegment;

public class MemTableIterator {
  private final SkipList skipList;
  private int current;

  public MemTableIterator(SkipList skipList) {
    this.skipList = skipList;
  }

  public void seek(MemorySegment key){
    current = skipList.get(key, Long.MAX_VALUE);
  }

  public void next(){
    current = skipList.readNextValid(current);
  }

  public boolean isValid(){
    return current != -1;
  }

  public void seekToFirst(){
    current = skipList.readNextValid(skipList.getHead());
  }
}
