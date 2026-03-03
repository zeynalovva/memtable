package az.zeynalov.memtable;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

public class SkipList {

  private final static float PROBABILITY = 0.25F;
  private final static int MAX_LEVEL = 12;

  private final static int PREFIX_LENGTH = 8;
  private final static int SN_LENGTH = 8;
  private final static int TYPE_LENGTH = 4;
  private final static int KEY_LENGTH = 4;
  private final static int VALUE_LENGTH = 4;
  private final static int LEVEL_COUNT_LENGTH = 4;
  private final static int POINTER_SIZE = 4;

  private final static int KEY_LENGTH_OFFSET = SN_LENGTH + TYPE_LENGTH;
  private final static int HOT_PATH_METADATA = PREFIX_LENGTH + LEVEL_COUNT_LENGTH + POINTER_SIZE;


  private final ThreadLocal<int[]> updateCache = ThreadLocal.withInitial(
      () -> new int[MAX_LEVEL + 1]);
  private final Arena hotArena;
  private final Arena coldArena;

  private int head;
  private int currenLevel;

  // TODO
  // TODO make code clean
  // TODO assure concurrency
  public SkipList(Arena hotArena, Arena coldArena) {
    this.hotArena = hotArena;
    this.coldArena = coldArena;
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
    int currentPosition = head;
    long targetPrefix = getPrefix(header.key());

    for (int i = currenLevel; i >= 0; i--) {
      while (true) {
        int next = readNext(i, currentPosition);
        if (isNull(next) || compare(next, targetPrefix, header.SN(), header.key()) <= 0) {
          break;
        }
        currentPosition = next;
      }
    }

    currentPosition = readNext(0, currentPosition);
    return (currentPosition != -1
        && compare(currentPosition, targetPrefix, header.SN(), header.key()) == 0) ? currentPosition
        : -1;
  }

  /**
   * Layout of node: prefix (8 bytes) + SN (8 bytes) + key size (4 bytes) + value size (4 bytes) +
   * level count (4 bytes) + next node pointers (4 bytes each) + key bytes + value bytes
   */
  public void insert(Header header, Footer footer) {
    int[] update = updateCache.get();
    int currentPosition = head;
    long targetPrefix = getPrefix(header.key());

    for (int i = currenLevel; i >= 0; i--) {
      while (true) {
        int next = readNext(i, currentPosition);
        if (isNull(next) || compare(next, targetPrefix, header.SN(), header.key()) <= 0) {
          break;
        }
        currentPosition = next;
      }

      update[i] = currentPosition;
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
      int nextOfUpdate = readNext(i, update[i]);
      writeNext(newNode, i, nextOfUpdate);
      writeNext(update[i], i, newNode);
    }
  }

  /**
   * Returns the offsets of the nodes in the arena if the keys are found
   */
  public void forEach(Consumer<Integer> consumer) {
    int currentNodePointer = readNext(0, head);
    while (!isNull(currentNodePointer)) {
      consumer.accept(currentNodePointer);
      currentNodePointer = readNext(0, currentNodePointer);
    }
  }


  /**
   * This method compares the key and SN of the node at the given offset with the target key and SN.
   * It reads the key size, key, and SN from the node and compares them with the target key and SN.
   * The method returns: - a negative integer if the node is less than the target (node key < target
   * key or node key == target key and node SN < target SN) - zero if the node is equal to the
   * target (node key == target key and node SN == target SN) - a positive integer if the node is
   * greater than the target (node key > target key or node key == target key and node SN > target
   * SN) The comparison is done first by key and then by SN if the keys are equal. The method uses
   * MemorySegment.mismatch to find the first byte where the node key and target key differ, which
   * allows for efficient comparison without needing to read the entire key if they differ early
   * on.
   */
  private int compare(int nodeOffset, long targetPrefix, long targetSN,
      MemorySegment targetKey) {
    int keyComparison = compareKeyOnly(nodeOffset, targetPrefix, targetKey);

    if (keyComparison == 0) {
      int tempDataOffset = nodeOffset + LEVEL_COUNT_LENGTH + PREFIX_LENGTH;
      int offset = hotArena.readInt(tempDataOffset);
      long SN = coldArena.readLong(offset);
      return Long.compare(SN, targetSN);
    }

    return keyComparison;
  }

  private int compareKeyOnly(int nodeOffset, long targetPrefix, MemorySegment targetKey) {
    long sourcePrefix = hotArena.readLong(nodeOffset);
    int comparison = Long.compareUnsigned(targetPrefix, sourcePrefix);
    if (comparison != 0) {
      return comparison;
    }

    return compareRawKeys(nodeOffset, targetKey);
  }

  private int compareRawKeys(int nodeOffset, MemorySegment targetKey) {
    int tempDataOffset = nodeOffset + LEVEL_COUNT_LENGTH + PREFIX_LENGTH;
    int offset = hotArena.readInt(tempDataOffset);
    int keyLength = coldArena.readInt(offset + KEY_LENGTH_OFFSET);
    int keyOffset = offset + KEY_LENGTH_OFFSET + KEY_LENGTH + VALUE_LENGTH;

    long mismatch = MemorySegment.mismatch(
        coldArena.getMemory(), keyOffset, keyOffset + keyLength,
        targetKey, 0, targetKey.byteSize()
    );

    if (mismatch == -1) {
      return 0;
    }

    if (mismatch == targetKey.byteSize()) {
      return 1;
    }
    if (mismatch == keyLength) {
      return -1;
    }

    byte b1 = coldArena.readByte(keyOffset + (int) mismatch);
    byte b2 = targetKey.get(ValueLayout.JAVA_BYTE, mismatch);
    return Byte.compareUnsigned(b1, b2);
  }

  /**
   * This method extracts the prefix from the given key. The prefix is defined as the first 8 bytes
   * of the key, which are used for efficient comparison in the skip list. If the key is shorter
   * than 8 bytes, the method constructs the prefix by reading the available bytes and padding the
   * rest with zeros.
   */

  public long getPrefix(MemorySegment key) {
    long size = key.byteSize();

    if (size >= 8) {
      return key.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
    }
    return switch ((int) size) {
      case 0 -> 0L;
      case 1 -> (long) (key.get(ValueLayout.JAVA_BYTE, 0) & 0xFF);
      case 2 -> (long) (key.get(ValueLayout.JAVA_SHORT_UNALIGNED, 0) & 0xFFFF);
      case 3 -> (long) (key.get(ValueLayout.JAVA_SHORT_UNALIGNED, 0) & 0xFFFF) |
          ((long) (key.get(ValueLayout.JAVA_BYTE, 2) & 0xFF) << 16);
      case 4 -> (key.get(ValueLayout.JAVA_INT_UNALIGNED, 0) & 0xFFFFFFFFL);
      case 5 -> (key.get(ValueLayout.JAVA_INT_UNALIGNED, 0) & 0xFFFFFFFFL) |
          ((long) (key.get(ValueLayout.JAVA_BYTE, 4) & 0xFF) << 32);
      case 6 -> (key.get(ValueLayout.JAVA_INT_UNALIGNED, 0) & 0xFFFFFFFFL) |
          ((long) (key.get(ValueLayout.JAVA_SHORT_UNALIGNED, 4) & 0xFFFF) << 32);
      case 7 -> (key.get(ValueLayout.JAVA_INT_UNALIGNED, 0) & 0xFFFFFFFFL) |
          ((long) (key.get(ValueLayout.JAVA_SHORT_UNALIGNED, 4) & 0xFFFF) << 32) |
          ((long) (key.get(ValueLayout.JAVA_BYTE, 6) & 0xFF) << 48);
      default -> throw new IllegalStateException("Unexpected value: " + (int) size);
    };
  }


  /**
   * This method creates a new node in the arena with the given header and footer information. It
   * calculates the total size needed for the node, allocates the memory in the arena and writes the
   * node's metadata (number of levels, next node pointers) and the record's header and footer
   * information into the allocated memory.
   * <p>
   * Code is split into three parts: writing node metadata, writing header and writing footer. We
   * can split the code into three methods if we want to make it more readable, but it will be less
   * efficient, because we will have to calculate offsets multiple times and write to the arena
   * multiple times.
   */

  private int createNodeWithRecord(int numberOfLevels, Header header, Footer footer) {
    final int coldDataSize = SN_LENGTH + TYPE_LENGTH + KEY_LENGTH + VALUE_LENGTH + header.keySize()
        + footer.valueSize();
    final int hotDataSize =
        PREFIX_LENGTH +LEVEL_COUNT_LENGTH + POINTER_SIZE + numberOfLevels * POINTER_SIZE;

    int offset = coldArena.allocate(coldDataSize);
    int temp = offset;
    long prefix = getPrefix(header.key());
    long SN = header.SN();
    byte type = header.type();
    int keySize = header.keySize();
    int valueSize = footer.valueSize();

    // Write cold data first
    coldArena.writeLong(offset, SN);
    offset += SN_LENGTH;
    coldArena.writeByte(offset, type);
    offset += TYPE_LENGTH;
    coldArena.writeInt(offset, keySize);
    offset += KEY_LENGTH;
    coldArena.writeInt(offset, valueSize);
    offset += VALUE_LENGTH;
    coldArena.writeBytes(offset, header.key());
    offset += header.keySize();
    coldArena.writeBytes(offset, footer.value());

    int newOffset = hotArena.allocate(hotDataSize);
    int tempOffset = newOffset;
    hotArena.writeLong(newOffset, prefix);
    newOffset += PREFIX_LENGTH;
    hotArena.writeInt(newOffset, numberOfLevels);
    newOffset += LEVEL_COUNT_LENGTH;
    hotArena.writeInt(newOffset, temp);
    newOffset += POINTER_SIZE;

    for (int i = 0; i < numberOfLevels; i++) {
      hotArena.writeInt(newOffset + (POINTER_SIZE * i), -1);
    }
    return tempOffset;
  }

  /**
   * Since we use arena approach, we cannot represent null values. Because we store offsets and
   * values in the arena, and offset cannot be negative, we can use -1 to represent null values.
   */
  private boolean isNull(int value) {
    return value == -1;
  }


  /**
   * This method is used to create a new node with the given number of levels and initialize the
   * next node pointers to -1 (null).
   */
  private int createNewNodePointers(int numberOfLevels) {
    int offset = hotArena.allocate(HOT_PATH_METADATA + numberOfLevels * POINTER_SIZE);
    hotArena.writeInt(offset + PREFIX_LENGTH, numberOfLevels);
    int tempOffset = offset + HOT_PATH_METADATA;
    for (int i = 0; i < numberOfLevels; i++) {
      hotArena.writeInt(tempOffset + (POINTER_SIZE * i), -1);
    }

    return offset;
  }

  /**
   * Instead of just jumping to the offset of the next node, this method reads the offset of the
   * next node at a specific level and returns it.
   */
  private int readNext(int index, int offset) {
    int nextNodeOffset = offset + HOT_PATH_METADATA + (POINTER_SIZE * index);
    return hotArena.readInt(nextNodeOffset);
  }

  private void writeNext(int nodeOffset, int level, int value) {
    hotArena.writeInt(nodeOffset + HOT_PATH_METADATA + (POINTER_SIZE * level), value);
  }

  /**
   * ThreadLocalRandom is used for optimal performance in concurrent environments. The method
   * generates a random level for a new node based on the defined probability and maximum level.
   */
  private int randomLevel() {
    int level = 0;
    while (level < MAX_LEVEL && ThreadLocalRandom.current().nextDouble() < PROBABILITY) {
      level++;
    }
    return level;
  }

}