package az.zeynalov.memtable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SkipList {

  private final static float PROBABILITY = 0.25F;
  private final static int MAX_LEVEL = 12;
  private final static int NODE_POINTER_SIZE = Integer.BYTES;
  private final static int NODE_ARRAY_LENGTH_SIZE = Integer.BYTES;

  private final Arena arena;
  private final Random random;

  private int head;
  private int currenLevel;

  /// TODO make this atomic

  public SkipList(Arena arena) {
    this.arena = arena;
    this.random = new Random();
    this.currenLevel = 0;
  }

  public void init() {
    this.head = createNewNodePointers(MAX_LEVEL);
  }

  public Header get(ByteBuffer key) {
    int currentNodePointer = head;

    for (int i = currenLevel; i >= 0; i--) {
      while (true) {
        int tempNodePointer = readIthNextNode(i, currentNodePointer);
        if (isNull(tempNodePointer)) {
          break;
        }
        Header tempHeader = getHeader(tempNodePointer);

        if (compareKeys(tempHeader.key(), key) >= 0) {
          break;
        }
        currentNodePointer = tempNodePointer;
      }
    }

    currentNodePointer = readIthNextNode(0, currentNodePointer);

    if (!isNull(currentNodePointer)) {
      Header tempHeader = getHeader(currentNodePointer);
      if (compareKeys(tempHeader.key(), key) == 0) {
        return tempHeader;
      }
    }
    return null;
  }

  public void insert(Header header) {
    int[] update = new int[MAX_LEVEL + 1];
    int currentNodePointer = head;

    for (int i = currenLevel; i >= 0; i--) {
      while (true) {
        int tempNodePointer = readIthNextNode(i, currentNodePointer);
        if (isNull(tempNodePointer)) {
          break;
        }
        Header tempHeader = getHeader(tempNodePointer);

        if (compareKeys(tempHeader.key(), header.key()) >= 0) {
          break;
        }
        currentNodePointer = tempNodePointer;
      }

      update[i] = currentNodePointer;
    }

    currentNodePointer = readIthNextNode(0, currentNodePointer);

    if (!isNull(currentNodePointer)) {
      Header tempHeader = getHeader(currentNodePointer);
      if (compareKeys(tempHeader.key(), header.key()) == 0) {
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

    int newNode = createNodeWithHeader(newLevel + 1, header);

    for (int i = 0; i <= newLevel; i++) {
      int nextNode = getIthNextNodeOffset(i, newNode);
      int updateNextNode = readIthNextNode(i, update[i]);
      arena.writeInt(nextNode, updateNextNode);
      int updateNodeOffset = getIthNextNodeOffset(i, update[i]);
      arena.writeInt(updateNodeOffset, newNode);
    }
  }

  private int compareKeys(ByteBuffer a, ByteBuffer b) {
    int aPos = a.position();
    int bPos = b.position();
    int aLim = a.limit();
    int bLim = b.limit();

    int len1 = aLim - aPos;
    int len2 = bLim - bPos;
    int minLength = Math.min(len1, len2);

    int mismatch = a.mismatch(b);

    if (mismatch >= 0 && mismatch < minLength) {
      return Byte.compareUnsigned(a.get(aPos + mismatch), b.get(bPos + mismatch));
    }

    return Integer.compare(len1, len2);
  }

  private Header getHeader(int offsetOfNode) {
    int headerOffset = skipNextNodePointers(offsetOfNode);
    Pair<Integer, Integer> keyPayload = arena.readVarint(headerOffset);

    int keySize = keyPayload.value();
    int keyOffset = headerOffset + keyPayload.numberOfBytes();

    ByteBuffer key = arena.read(keyOffset, keySize);
    int SN_Offset = keyOffset + keySize;
    int SN = arena.readInt(SN_Offset);

    return new Header(keySize, key, SN);
  }

  private void writeHeader(Header header) {
    int varintSize = arena.getVarintSize(header.keySize());
    int keySize = header.keySize();
    int SN_Size = Integer.BYTES;

    int totalSize = varintSize + keySize + SN_Size;

    int offset = arena.allocate(totalSize);

    arena.writeVarint(header.keySize(), offset);
    offset += varintSize;
    arena.write(offset, header.key());
    offset += keySize;
    arena.writeInt(offset, header.SN());
  }

  private int createNodeWithHeader(int numberOfLevels, Header header) {
    int varintSize = arena.getVarintSize(header.keySize());
    int keySize = header.keySize();
    int SN_Size = Integer.BYTES;

    int nodePointersSize = NODE_ARRAY_LENGTH_SIZE + numberOfLevels * NODE_POINTER_SIZE;
    int headerSize = varintSize + keySize + SN_Size;
    int totalSize = nodePointersSize + headerSize;

    int offset = arena.allocate(totalSize);

    arena.writeInt(offset, numberOfLevels);
    int tempOffset = offset + NODE_ARRAY_LENGTH_SIZE;
    for (int i = 0; i < numberOfLevels; i++) {
      arena.writeInt(tempOffset + (NODE_POINTER_SIZE * i), -1);
    }

    int headerOffset = offset + nodePointersSize;
    arena.writeVarint(header.keySize(), headerOffset);
    headerOffset += varintSize;
    arena.write(headerOffset, header.key());
    headerOffset += keySize;
    arena.writeInt(headerOffset, header.SN());

    return offset;
  }

  private int skipNextNodePointers(int offsetOfNode) {
    int sizeOfNodes = arena.readInt(offsetOfNode);
    return offsetOfNode + NODE_ARRAY_LENGTH_SIZE + NODE_POINTER_SIZE * sizeOfNodes;
  }

  private boolean isNull(int value) {
    return value == -1;
  }

  private List<Integer> readNextNodes(int offset) {
    int sizeOfNodes = arena.readInt(offset);
    List<Integer> temp = new ArrayList<>();
    for (int i = 0; i < sizeOfNodes; i++) {
      temp.add(readIthNextNode(i, offset));
    }

    return temp;
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
    while (level < MAX_LEVEL && random.nextDouble() < PROBABILITY) {
      level++;
    }
    return level;
  }

}
