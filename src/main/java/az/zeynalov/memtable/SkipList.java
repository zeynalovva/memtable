package az.zeynalov.memtable;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

public class SkipList {

  private final static float PROBABILITY = 0.25F;
  private final static int MAX_LEVEL = 12;
  private final static int NODE_POINTER_SIZE = Integer.BYTES;
  private final static int NODE_ARRAY_LENGTH_SIZE = Integer.BYTES;
  private final static int OPERATION_TYPE_SIZE = Byte.BYTES;
  private final ThreadLocal<int[]> updateNodesCache = ThreadLocal.withInitial(() -> new int[MAX_LEVEL + 1]);


  private final Arena arena;

  private int head;
  private int currenLevel;

  // TODO make code clean
  // TODO assure concurrency
  public SkipList(Arena arena) {
    this.arena = arena;
    this.currenLevel = 0;
  }

  public void init() {
    this.head = createNewNodePointers(MAX_LEVEL);
  }

  public Record get(Header header) {
    int currentNodePointer = head;

    for (int i = currenLevel; i >= 0; i--) {
      while (true) {
        int tempNodePointer = readIthNextNode(i, currentNodePointer);
        if (isNull(tempNodePointer)) {
          break;
        }

        if (compareNodeWithTarget(tempNodePointer, header.key(), header.SN()) >= 0) {
          break;
        }
        currentNodePointer = tempNodePointer;
      }
    }

    currentNodePointer = readIthNextNode(0, currentNodePointer);

    if (!isNull(currentNodePointer)) {
      if (compareNodeWithTarget(currentNodePointer, header.key(), header.SN()) == 0) {
        Footer tempFooter = getFooter(currentNodePointer);
        Header tempHeader = getHeader(currentNodePointer);
        return new Record(tempHeader, tempFooter);
      }
    }
    return null;
  }

  public void insert(Record record) {
    int[] update = updateNodesCache.get();
    int currentNodePointer = head;

    for (int i = currenLevel; i >= 0; i--) {
      while (true) {
        int tempNodePointer = readIthNextNode(i, currentNodePointer);
        if (isNull(tempNodePointer)) {
          break;
        }
        if (compareNodeWithTarget(tempNodePointer, record.header().key(), record.header().SN())
            >= 0) {
          break;
        }
        currentNodePointer = tempNodePointer;
      }

      update[i] = currentNodePointer;
    }

    currentNodePointer = readIthNextNode(0, currentNodePointer);

    if (!isNull(currentNodePointer)) {
      if (compareNodeWithTarget(currentNodePointer, record.header().key(), record.header().SN())
          == 0) {
        return;
      }
    }

    int newLevel = randomLevel();

    if (newLevel > currenLevel) {
      for (int i = currenLevel + 1; i <= newLevel; i++) {
        update[i] = head;
      }
      currenLevel = newLevel;
    }

    int newNode = createNodeWithRecord(newLevel + 1, record);

    for (int i = 0; i <= newLevel; i++) {
      int nextNode = getIthNextNodeOffset(i, newNode);
      int updateNextNode = readIthNextNode(i, update[i]);
      arena.writeInt(nextNode, updateNextNode);
      int updateNodeOffset = getIthNextNodeOffset(i, update[i]);
      arena.writeInt(updateNodeOffset, newNode);
    }
  }

  public void forEach(Consumer<Header> consumer) {
    int currentNodePointer = readIthNextNode(0, head);
    while (!isNull(currentNodePointer)) {
      Header header = getHeader(currentNodePointer);
      consumer.accept(header);
      currentNodePointer = readIthNextNode(0, currentNodePointer);
    }
  }

  public int compareHeaders(Header a, Header b) {
    long mismatchOffset = a.key().mismatch(b.key());

    if (mismatchOffset == -1) {
      return Long.compare(b.SN(), a.SN());
    }

    if (mismatchOffset == a.key().byteSize() || mismatchOffset == b.key().byteSize()) {
      return Long.compare(a.key().byteSize(), b.key().byteSize());
    }

    return Byte.compareUnsigned(
        a.key().get(ValueLayout.JAVA_BYTE, mismatchOffset),
        b.key().get(ValueLayout.JAVA_BYTE, mismatchOffset)
    );
  }

  public int compareNodeWithTarget(int nodeOffset, MemorySegment targetKey, long targetSN) {
    int sizeOfNodes = arena.readInt(nodeOffset);
    int metadataOffset = nodeOffset + NODE_ARRAY_LENGTH_SIZE + (NODE_POINTER_SIZE * sizeOfNodes);

    int currentPos = metadataOffset;
    int keySize = 0;
    int shift = 0;
    while (true) {
      byte b = arena.readByte(currentPos++);
      keySize |= (b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        break;
      }
      shift += 7;
    }

    long mismatch = MemorySegment.mismatch(
        arena.getMemory(), currentPos, currentPos + keySize,
        targetKey, 0, targetKey.byteSize()
    );

    if (mismatch == -1) {
      long nodeSN = arena.readLong(currentPos + keySize);
      return Long.compare(targetSN, nodeSN);
    }

    if (mismatch == targetKey.byteSize()) {
      return 1;
    }
    if (mismatch == keySize) {
      return -1;
    }

    byte b1 = arena.readByte(currentPos + (int) mismatch);
    byte b2 = targetKey.get(ValueLayout.JAVA_BYTE, mismatch);
    return Byte.compareUnsigned(b1, b2);
  }

  private Header getHeader(int offsetOfNode) {
    int headerOffset = skipNextNodePointers(offsetOfNode);
    long keyPayload = arena.readVarint(headerOffset);

    int keySize = unpackFirst(keyPayload);
    int keyOffset = headerOffset + unpackSecond(keyPayload);
    int SN_Size = Long.BYTES;

    MemorySegment key = arena.readBytes(keyOffset, keySize);
    int SN_Offset = keyOffset + keySize;
    long SN = arena.readLong(SN_Offset);
    int typeOffset = SN_Offset + SN_Size;
    byte type = arena.readByte(typeOffset);

    return new Header(keySize, key, SN, type);
  }

  private Footer getFooter(int offsetOfNode) {
    int headerOffset = skipNextNodePointers(offsetOfNode);
    long keyPayload = arena.readVarint(headerOffset);
    int keySize = unpackFirst(keyPayload);
    int keyVarintSize = unpackSecond(keyPayload);

    int SN_Size = Long.BYTES;

    int footerOffset = headerOffset + keyVarintSize +
        keySize + SN_Size + OPERATION_TYPE_SIZE;

    long valuePayload = arena.readVarint(footerOffset);

    int valueSize = unpackFirst(valuePayload);
    int valueOffset = footerOffset + unpackSecond(valuePayload);
    MemorySegment value = arena.readBytes(valueOffset, valueSize);

    return new Footer(valueSize, value);
  }

  private int createNodeWithRecord(int numberOfLevels, Record record) {
    Header header = record.header();
    Footer footer = record.footer();

    int keyVarintSize = getVarintSize(header.keySize());
    int keySize = header.keySize();
    int SN_Size = Long.BYTES;
    int typeSize = Byte.BYTES;
    int valueVarintSize = getVarintSize(footer.valueSize());
    int valueSize = footer.valueSize();

    int nodePointersSize = NODE_ARRAY_LENGTH_SIZE + numberOfLevels * NODE_POINTER_SIZE;
    int headerSize = keyVarintSize + keySize + SN_Size + typeSize;
    int footerSize = valueVarintSize + valueSize;
    int totalSize = nodePointersSize + headerSize + footerSize;

    int offset = arena.allocate(totalSize);

    arena.writeInt(offset, numberOfLevels);
    int tempOffset = offset + NODE_ARRAY_LENGTH_SIZE;
    for (int i = 0; i < numberOfLevels; i++) {
      arena.writeInt(tempOffset + (NODE_POINTER_SIZE * i), -1);
    }

    int headerOffset = offset + nodePointersSize;
    arena.writeVarint(headerOffset, header.keySize());
    headerOffset += keyVarintSize;
    arena.writeBytes(headerOffset, header.key());
    headerOffset += keySize;
    arena.writeLong(headerOffset, header.SN());
    headerOffset += SN_Size;
    arena.writeByte(headerOffset, header.type());
    headerOffset += typeSize;
    arena.writeVarint(headerOffset, footer.valueSize());
    headerOffset += valueVarintSize;
    arena.writeBytes(headerOffset, footer.value());

    return offset;
  }

  private int skipNextNodePointers(int offsetOfNode) {
    int sizeOfNodes = arena.readInt(offsetOfNode);
    return offsetOfNode + NODE_ARRAY_LENGTH_SIZE + NODE_POINTER_SIZE * sizeOfNodes;
  }

  private boolean isNull(int value) {
    return value == -1;
  }

  private int createNewNodePointers(int numberOfLevels) {
    int offset = arena.allocate(NODE_ARRAY_LENGTH_SIZE + numberOfLevels * NODE_POINTER_SIZE);
    arena.writeInt(offset, numberOfLevels);
    int tempOffset = offset + NODE_ARRAY_LENGTH_SIZE;
    for (int i = 0; i < numberOfLevels; i++) {
      arena.writeInt(tempOffset + (NODE_POINTER_SIZE * i), -1);
    }

    return offset;
  }

  private int readIthNextNode(int index, int offset) {
    int nextNodeOffset = offset + (NODE_ARRAY_LENGTH_SIZE + NODE_POINTER_SIZE * index);
    return arena.readInt(nextNodeOffset);
  }

  private int getIthNextNodeOffset(int index, int offset) {
    return offset + (NODE_ARRAY_LENGTH_SIZE + NODE_POINTER_SIZE * index);
  }

  private int randomLevel() {
    int level = 0;
    while (level < MAX_LEVEL && ThreadLocalRandom.current().nextDouble() < PROBABILITY) {
      level++;
    }
    return level;
  }

  private int getVarintSize(int value) {
    if ((value & (0xFFFFFFFF << 7)) == 0) {
      return 1;
    }
    if ((value & (0xFFFFFFFF << 14)) == 0) {
      return 2;
    }
    if ((value & (0xFFFFFFFF << 21)) == 0) {
      return 3;
    }
    if ((value & (0xFFFFFFFF << 28)) == 0) {
      return 4;
    }
    return 5;
  }

  private long pack(int a, int b) {
    return ((long) a << 32) | (b & 0xFFFFFFFFL);
  }

  private int unpackFirst(long packed) {
    return (int) (packed >> 32);
  }

  private int unpackSecond(long packed) {
    return (int) packed;
  }
}
