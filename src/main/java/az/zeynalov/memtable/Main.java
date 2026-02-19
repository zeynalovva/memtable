package az.zeynalov.memtable;


import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;

public class Main {

  private static final ValueLayout.OfInt UNALIGNED_INT =
      ValueLayout.JAVA_INT.withByteAlignment(1);

  public static void main(String[] args) {

    ArenaImpl arenaImpl = new ArenaImpl();
    SkipList skipList = new SkipList(arenaImpl);
    skipList.init();

    int key = 10000;
    int SN = 120;
    Header header = new Header(4, toMemorySegment(key), SN);
    skipList.insert(header);


    key = 11;
    SN = 114;
    header = new Header(4, toMemorySegment(key), SN);
    skipList.insert(header);

    key = 13;
    SN = 112;
    header = new Header(4, toMemorySegment(key), SN);
    skipList.insert(header);

    key = 50;
    SN = 150;
    header = new Header(4, toMemorySegment(key), SN);
    skipList.insert(header);

    key = 40;
    SN = 140;
    header = new Header(4, toMemorySegment(key), SN);
    skipList.insert(header);


  }

  public static MemorySegment toMemorySegment(int key) {
    MemorySegment segment = MemorySegment.ofArray(new byte[Integer.BYTES]);

    segment.set(UNALIGNED_INT, 0, key);

    return segment;
  }

  public static int toInt(MemorySegment segment) {
    return segment.get(ValueLayout.JAVA_INT, 0);
  }

}
