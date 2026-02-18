package az.zeynalov.tests;

import static az.zeynalov.memtable.Main.toByteBuffer;
import static az.zeynalov.memtable.Main.toInt;

import az.zeynalov.memtable.Arena;
import az.zeynalov.memtable.Header;
import az.zeynalov.memtable.Pair;
import az.zeynalov.memtable.SkipList;
import java.nio.ByteBuffer;

public class SkipListTest {
  private final Arena arena;
  private final SkipList skipList;

  public SkipListTest(Arena arena, SkipList skipList) {
    this.arena = arena;
    this.skipList = skipList;
  }


  public void testHeaderWrite(){
    int key = 10000;
    int SN = 120;
    int k = arena.getVarintSize(key);
    Header header = new Header(k, toByteBuffer(key), SN);


    arena.write(0, header.key());



  }
}
