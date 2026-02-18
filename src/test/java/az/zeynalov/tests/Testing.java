package az.zeynalov.tests;

import az.zeynalov.memtable.Arena;
import az.zeynalov.memtable.SkipList;

public class Testing {

  public static void main(String[] args) {
    Arena arena = new Arena();
    SkipList skipList = new SkipList(arena);
    SkipListTest test = new SkipListTest(arena, skipList);

    test.testHeaderWrite();
  }

}
