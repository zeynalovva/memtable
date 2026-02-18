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
    Header header = new Header(4, toByteBuffer(key), SN);
    skipList.insert(header);


    key = 11;
    SN = 114;
    keySize = arena.getVarintSize(key);
    header = new Header(4, toByteBuffer(key), SN);
    skipList.insert(header);

    key = 13;
    SN = 112;
    keySize = arena.getVarintSize(key);
    header = new Header(4, toByteBuffer(key), SN);
    skipList.insert(header);

    key = 50;
    SN = 150;
    keySize = arena.getVarintSize(key);
    header = new Header(4, toByteBuffer(key), SN);
    skipList.insert(header);

    key = 40;
    SN = 140;
    keySize = arena.getVarintSize(key);
    header = new Header(4, toByteBuffer(key), SN);
    skipList.insert(header);

    Header out = skipList.get(toByteBuffer(40));

    System.out.println(out.keySize() + " " + out.SN());
  }

  public static ByteBuffer toByteBuffer(int key){
    ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
    buffer.putInt(key);
    buffer.flip();
    return buffer;
  }


  public static int toInt(ByteBuffer buffer){
    return buffer.getInt();
  }

}
