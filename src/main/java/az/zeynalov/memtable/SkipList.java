package az.zeynalov.memtable;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

public class SkipList {

  private final static float PROBABILITY = 0.25F;
  private final static int MAX_LEVEL = 12;
  private final static int NODE_POINTER_SIZE = Integer.BYTES;
  private final static int NODE_ARRAY_LENGTH_SIZE = Integer.BYTES;
  private final static int DEFAULT_SN_SIZE = Long.BYTES;
  private final static int DEFAULT_TYPE_SIZE = Byte.BYTES;

  private final ThreadLocal<int[]> updateNodesCache = ThreadLocal.withInitial(
      () -> new int[MAX_LEVEL + 1]);
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

  // Returns the offset according to MVCC
  // (user:42, 130, tombstone)
  // (user:42, 122, "Al")
  // (user:42, 120, "Alicia")
  // If a key with the value of 42 and SN of 125 is searched,
  // the offset of the second record should be returned, because
  // it is the latest version of the key that is less than or
  // equal to the searched SN.
  // It returns -1 if the key is not found or all versions of the key have SN greater than the searched SN.
  public int get(Header header) {
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

    return currentNodePointer;
  }

  /**
   * Layout of node: [number of levels] -> [next node pointer for each level] -> -> [key varint
   * size] -> [key] -> [SN] -> [type] -> [value varint size] -> -> [value]
   */
  public void insert(Header header, Footer footer) {
    int[] update = updateNodesCache.get();
    int currentNodePointer = head;

    for (int i = currenLevel; i >= 0; i--) {
      while (true) {
        int tempNodePointer = readIthNextNode(i, currentNodePointer);
        if (isNull(tempNodePointer)) {
          break;
        }
        if (compareNodeWithTarget(tempNodePointer, header.key(), header.SN())
            >= 0) {
          break;
        }
        currentNodePointer = tempNodePointer;
      }

      update[i] = currentNodePointer;
    }

    // Reset to level 0 to check if the key already exists at the lowest level
    currentNodePointer = readIthNextNode(0, currentNodePointer);

    if (!isNull(currentNodePointer)) {
      if (compareNodeWithTarget(currentNodePointer, header.key(), header.SN())
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

    int newNode = createNodeWithRecord(newLevel + 1, header, footer);

    // Interchange update and new node pointers
    for (int i = 0; i <= newLevel; i++) {
      int nextNode = getIthNextNodeOffset(i, newNode);
      int updateNextNode = readIthNextNode(i, update[i]);
      arena.writeInt(nextNode, updateNextNode);
      int updateNodeOffset = getIthNextNodeOffset(i, update[i]);
      arena.writeInt(updateNodeOffset, newNode);
    }
  }

  /**
   * Returns the offsets of the nodes in the arena if the keys are found
   */
  public void forEach(Consumer<Integer> consumer) {
    int currentNodePointer = readIthNextNode(0, head);
    while (!isNull(currentNodePointer)) {
      consumer.accept(currentNodePointer);
      currentNodePointer = readIthNextNode(0, currentNodePointer);
    }
  }


  /**
   * This method compares the key and SN of the node at the given offset with the target key and SN.
   * It reads the key size, key, and SN from the node and compares them with the target key and SN.
   * The method returns:
   * - a negative integer if the node is less than the target (node key < target key or node key == target key and node SN < target SN)
   * - zero if the node is equal to the target (node key == target key and node SN == target SN)
   * - a positive integer if the node is greater than the target (node key > target key or node key == target key and node SN > target SN)
   * The comparison is done first by key and then by SN if the keys are equal.
   * The method uses MemorySegment.mismatch to find the first byte where the node key and target key differ,
   * which allows for efficient comparison without needing to read the entire key if they differ early on.
   */
  private int compareNodeWithTarget(int nodeOffset, MemorySegment targetKey, long targetSN) {
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


  /**
   * This method creates a new node in the arena with the given header and footer information.
   * It calculates the total size needed for the node, allocates the memory in the arena
   * and writes the node's metadata (number of levels, next node pointers) and the record's
   * header and footer information into the allocated memory.
   *
   * Code is split into three parts: writing node metadata, writing header and writing footer.
   * We can split the code into three methods if we want to make it more readable,
   * but it will be less efficient, because we will have to calculate offsets multiple times and write to the arena multiple times.
   */
  private int createNodeWithRecord(int numberOfLevels, Header header, Footer footer) {

    int keyVarintSize = getVarintSize(header.keySize());
    int keySize = header.keySize();
    int SN_Size = DEFAULT_SN_SIZE;
    int typeSize = DEFAULT_TYPE_SIZE;
    int valueVarintSize = getVarintSize(footer.valueSize());
    int valueSize = footer.valueSize();

    int nodePointersSize = NODE_ARRAY_LENGTH_SIZE + numberOfLevels * NODE_POINTER_SIZE;
    int headerSize = keyVarintSize + keySize + SN_Size + typeSize;
    int footerSize = valueVarintSize + valueSize;
    final int totalSize = nodePointersSize + headerSize + footerSize;

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

  /**
   * Since we use arena approach, we cannot represent null values.
   * Because we store offsets and values in the arena,
   * and offset cannot be negative, we can use -1 to represent null values.
   */
  private boolean isNull(int value) {
    return value == -1;
  }


  /**
   * This method is used to create a new node with the given
   * number of levels and initialize the next node pointers to -1 (null).
   */
  private int createNewNodePointers(int numberOfLevels) {
    int offset = arena.allocate(NODE_ARRAY_LENGTH_SIZE + numberOfLevels * NODE_POINTER_SIZE);
    arena.writeInt(offset, numberOfLevels);
    int tempOffset = offset + NODE_ARRAY_LENGTH_SIZE;
    for (int i = 0; i < numberOfLevels; i++) {
      arena.writeInt(tempOffset + (NODE_POINTER_SIZE * i), -1);
    }

    return offset;
  }

  /**
   * Instead of just jumping to the offset of the next node,
   * this method reads the offset of the next node at a specific level and returns it.
   */
  private int readIthNextNode(int index, int offset) {
    int nextNodeOffset = offset + (NODE_ARRAY_LENGTH_SIZE + NODE_POINTER_SIZE * index);
    return arena.readInt(nextNodeOffset);
  }

  /**
   * Helps to jump to the offset of the next node at a specific level
   */
  private int getIthNextNodeOffset(int index, int offset) {
    return offset + (NODE_ARRAY_LENGTH_SIZE + NODE_POINTER_SIZE * index);
  }

  /**
   * ThreadLocalRandom is used for optimal performance in concurrent
   * environments. The method generates a random level for a new node
   * based on the defined probability and maximum level.
   */
  private int randomLevel() {
    int level = 0;
    while (level < MAX_LEVEL && ThreadLocalRandom.current().nextDouble() < PROBABILITY) {
      level++;
    }
    return level;
  }

  /**
   * Returns how many bytes a value can be encoded in varint format.
   */
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
}
