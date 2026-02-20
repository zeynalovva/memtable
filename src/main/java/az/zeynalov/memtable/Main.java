package az.zeynalov.memtable;


import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;

public class Main {

  private static final ValueLayout.OfInt UNALIGNED_INT =
      ValueLayout.JAVA_INT.withByteAlignment(1);

  public static void main(String[] args) {



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
