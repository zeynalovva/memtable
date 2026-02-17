package az.zeynalov.memtable;


import java.nio.ByteBuffer;

public class Main {

  public static void main(String[] args) {

    Arena arena = new Arena();
    SkipList skipList = new SkipList(arena);
    skipList.init();

    int key = 10000;
    int SN = 120;
    int keySize = arena.getVarintSize(key);
    Header header = new Header(keySize, toByteBuffer(key), SN);
    skipList.insert(header);

    key = 11;
    SN = 120;
    keySize = arena.getVarintSize(key);
    header = new Header(keySize, toByteBuffer(key), SN);
    skipList.insert(header);

    Header out = skipList.get(toByteBuffer(11));

    System.out.println(out.keySize() + out.SN());
  }

  public static ByteBuffer toByteBuffer(int key){
    ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
    buffer.putInt(key);
    return buffer;
  }

  public static int toInt(ByteBuffer buffer){
    return buffer.getInt();
  }

}
