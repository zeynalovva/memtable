package az.zeynalov.memtable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class SkipList {

  private final static float PROBABILITY = 0.25F;
  private final static int MAX_LEVEL = 12;
  private final static int NODE_POINTER_SIZE = Integer.BYTES;
  private final static int NODE_ARRAY_LENGTH_SIZE = Integer.BYTES;

  private Arena arena;
  private Random random;

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
        List<Integer> currentNodeNextPointers = readNextNodes(currentNodePointer);
        int tempOffset = currentNodeNextPointers.get(i);
        if (arena.isNull(tempOffset)) {
          break;
        }
        Header tempHeader = getHeader(tempOffset);

        if (compareKeys(tempHeader.key(), key) >= 0) {
          break;
        }
        currentNodePointer = tempOffset;
      }
    }

    currentNodePointer = currentNodePointer + Integer.BYTES;

    boolean isNull = (arena.readInt(currentNodePointer) == -1);

    if (!isNull) {
      Header tempHeader = getHeader(currentNodePointer);
      if (compareKeys(tempHeader.key(), key) == 0) {
        return tempHeader;
      }
    }
    return null;
  }

  public void insert(Header header) {
    List<Integer> update = new ArrayList<>(Collections.nCopies(MAX_LEVEL + 1, null));
    int currentNodePointer = head;

    for (int i = currenLevel; i >= 0; i--) {
      while (true) {
        List<Integer> currentNodeNextPointers = readNextNodes(currentNodePointer);
        int tempOffset = currentNodeNextPointers.get(i);
        if (arena.isNull(tempOffset)) {
          break;
        }
        Header tempHeader = getHeader(tempOffset);

        if (compareKeys(tempHeader.key(), header.key()) >= 0) {
          break;
        }
        currentNodePointer = tempOffset;
      }

      update.set(i, currentNodePointer);
    }

    currentNodePointer = currentNodePointer + Integer.BYTES;

    boolean isNull = (arena.readInt(currentNodePointer) == -1);

    if (!isNull) {
      Header tempHeader = getHeader(currentNodePointer);
      if (compareKeys(tempHeader.key(), header.key()) == 0) {
        return;
      }
    }

    int newLevel = randomLevel();

    if (newLevel > currenLevel) {
      for (int i = currenLevel + 1; i <= newLevel; i++) {
        update.set(i, head);
      }
      currenLevel = newLevel;
    }

    int newNode = createNewNodePointers(newLevel + 1);

    writeHeader(header);

    List<Integer> newNodeNextPointers = readNextNodeOffsets(newNode);

    for (int i = 0; i <= newLevel; i++) {
      int nextNode = newNodeNextPointers.get(i);
      int updateNextNode = readIthNextNode(i, update.get(i));
      arena.writeInt(nextNode, updateNextNode);
      arena.writeInt(getIthNextNodeOffset(i, update.get(i)), newNode);
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

    return len1 - len2;
  }

  private Header getHeader(int offsetOfNode) {
    int headerOffset = skipNextNodePointers(offsetOfNode);
    Pair<Integer, Integer> keyPayload = arena.readVarint(headerOffset);

    int keySize = keyPayload.first();
    int keyOffset = headerOffset + keyPayload.second();

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


  private int skipNextNodePointers(int offsetOfNode) {
    int sizeOfNodes = arena.readInt(offsetOfNode);
    return offsetOfNode + (sizeOfNodes + 1) * NODE_POINTER_SIZE;
  }

  private List<Integer> readNextNodeOffsets(int offset) {
    int sizeOfNodes = arena.readInt(offset);
    List<Integer> temp = new ArrayList<>();
    int tempOffset = offset + NODE_POINTER_SIZE;

    for (int i = 0; i < sizeOfNodes; i++) {
      temp.add(tempOffset + NODE_POINTER_SIZE * i);
    }
    return temp;
  }

  private List<Integer> readNextNodes(int offset) {
    int sizeOfNodes = arena.readInt(offset);
    List<Integer> temp = new ArrayList<>();
    for (int i = 0; i < sizeOfNodes; i++) {
      temp.add(readIthNextNode(i, offset));
    }

    return temp;
  }

  private int createNewNodePointers(int level) {
    int offset = arena.allocate((level + 1) * NODE_POINTER_SIZE);
    arena.writeInt(offset, level);
    int tempOffset = offset + NODE_POINTER_SIZE;
    for (int i = 0; i < level; i++) {
      arena.writeInt(tempOffset + (NODE_POINTER_SIZE * i), -1);
    }

    return offset;
  }

  private int readIthNextNode(int index, int offset) {
    int nodeOffset = offset + (NODE_POINTER_SIZE * (index + 1));
    return arena.readInt(nodeOffset);
  }

  private int getIthNextNodeOffset(int index, int offset) {
    return offset + (NODE_POINTER_SIZE * (index + 1));
  }


  private int randomLevel() {
    int level = 0;
    while (level < MAX_LEVEL && random.nextDouble() < PROBABILITY) {
      level++;
    }
    return level;
  }

}
