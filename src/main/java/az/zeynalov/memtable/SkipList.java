package az.zeynalov.memtable;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
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

  private static final VarHandle LEVEL_HANDLE;
  private static final VarHandle UPDATE_CACHE_HANDLE = ValueLayout.JAVA_INT.withOrder(ByteOrder.BIG_ENDIAN).varHandle();

  private final ThreadLocal<int[]> updateCache = ThreadLocal.withInitial(
      () -> new int[MAX_LEVEL + 1]);
  private final Arena hotArena;
  private final Arena coldArena;

  private int head;
  private int currenLevel;

  static{
    try{
      LEVEL_HANDLE = MethodHandles.lookup().findVarHandle(SkipList.class, "currenLevel", int.class);
    }
    catch (ReflectiveOperationException e){
      throw new Error(e);
    }
  }

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

    for (int i = (int) LEVEL_HANDLE.get(this); i >= 0; i--) {
      while (true) {
        int next = readNext(i, currentPosition);
        if (isNull(next) || compare(next, targetPrefix, header.SN(), header.key()) <= 0) {
          break;
        }
        currentPosition = next;
      }
    }

    int candidate = readNext(0, currentPosition);
    if (isNull(candidate)) {
      return -1;
    }

    if (compare(candidate, targetPrefix, header.SN(), header.key()) <= 0
        && compareKeyOnly(candidate, targetPrefix, header.key()) == 0) {
      return candidate;
    }

    return -1;
  }

  /**
   * Layout of the node in hot arena: [prefix (8 bytes)][level count (4 bytes)][offset to cold data (4 bytes)][next pointers...]
   * Layout of the node in cold arena: [SN (8 bytes)][type (4 bytes)][key size (4 bytes)][value size (4 bytes)][key bytes][value bytes]
   * The insert method first finds the correct position for the new node by traversing the skip
   * list levels, then it creates a new node with the given header and footer, and finally it
   * updates the next pointers of the new node and the existing nodes to maintain the skip list
   * structure. The method uses the updateCache to store the offsets of the nodes that need to be
   * updated at each level, which helps to efficiently update the pointers after inserting the new node.
   */
  public void insert(Header header, Footer footer) {
    int[] update = updateCache.get();
    int currentPosition = head;
    long targetPrefix = getPrefix(header.key());

    for (int i = (int) LEVEL_HANDLE.get(this); i >= 0; i--) {
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
    int oldLevel = (int) LEVEL_HANDLE.get(this);
    if (newLevel > oldLevel) {
      for (int i = oldLevel + 1; i <= newLevel; i++) {
        update[i] = head;
      }

      int witness;
      while ((witness = (int) LEVEL_HANDLE.get(this)) < newLevel) {
        if ((boolean) LEVEL_HANDLE.compareAndSet(this, witness, newLevel)) {
          break;
        }
      }
    }

    int newNode = createNodeWithRecord(newLevel + 1, header, footer);

    // Link new node into each level bottom-up using CAS.
    // 1. Set newNode's forward pointer to what we expect predecessor's next to be.
    // 2. CAS predecessor's next from that expected value to newNode.
    // 3. If CAS fails, re-traverse from update[i] at level i to find the
    //    correct predecessor (the node whose key is still < our key),
    //    then retry. This handles the case where another thread inserted
    //    a node between update[i] and the old next.
    for (int i = 0; i <= newLevel; i++) {
      while (true) {
        int expected = readNext(i, update[i]);

        if (!isNull(expected) && compare(expected, targetPrefix, header.SN(), header.key()) > 0) {
          int cur = update[i];
          while (true) {
            int next = readNext(i, cur);
            if (isNull(next) || compare(next, targetPrefix, header.SN(), header.key()) <= 0) {
              break;
            }
            cur = next;
          }
          update[i] = cur;
          continue;
        }

        writeNext(newNode, i, expected);
        if (casNext(update[i], i, expected, newNode)) {
          break;
        }
      }
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
   * To make it efficient, it first compares the prefix (the first 8 bytes of the key) and if they
   * are equal, it then compares the full keys and SNs. The method returns: - a negative integer if the node
   * is less than the target (node key < target key or node key == target key and node SN < target SN) - zero
   * if the node is equal to the target (node key == target key and node SN == target SN) - a positive
   * integer if the node is greater than the target (node key > target key or node key == target key
   * and node SN > target SN) The comparison is done first by key and then by SN if the keys are equal.
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

    if (keyLength < 32) {
      long targetLenLong = targetKey.byteSize();
      int targetLen = targetLenLong > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) targetLenLong;
      int minLen = Math.min(keyLength, targetLen);

      for (int i = 0; i < minLen; i++) {
        byte b1 = coldArena.readByte(keyOffset + i);
        byte b2 = targetKey.get(ValueLayout.JAVA_BYTE, i);
        int cmp = Byte.compareUnsigned(b1, b2);
        if (cmp != 0) {
          return cmp;
        }
      }

      return Integer.compare(keyLength, targetLen);
    }

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
   * This method creates a new node in the skip list with the given number of levels and the record
   * defined by the header and footer. It first calculates the sizes of the cold and hot data, then it
   * allocates space in the respective arenas and writes the data. The cold data includes the SN, type,
   * key size, value size, key bytes, and value bytes, while the hot data includes the prefix, level count,
   * offset to the cold data, and the next node pointers. Finally, it returns the offset of the newly
   * created node in the hot arena.
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
   * next node at a specific level and returns it. Uses acquire semantics to see writes from other threads.
   */
  private int readNext(int index, int offset) {
    int nextNodeOffset = offset + HOT_PATH_METADATA + (POINTER_SIZE * index);
    return (int) UPDATE_CACHE_HANDLE.getAcquire(hotArena.getMemory(), (long) nextNodeOffset);
  }

  /**
   * This method writes the offset of the next node at a specific level for a given node. It calculates
   * the correct position in the hot arena based on the node's offset and the level index, and then
   * writes the new offset value.
   */
  private void writeNext(int nodeOffset, int level, int value) {
    int nextNodeOffset = nodeOffset + HOT_PATH_METADATA + (POINTER_SIZE * level);
    UPDATE_CACHE_HANDLE.setRelease(hotArena.getMemory(), (long) nextNodeOffset, value);
  }

  /**
   * Atomically compare-and-swap the next pointer at a given level for a node.
   * Returns true if the CAS succeeded.
   */
  private boolean casNext(int nodeOffset, int level, int expectedValue, int newValue) {
    int nextNodeOffset = nodeOffset + HOT_PATH_METADATA + (POINTER_SIZE * level);
    return UPDATE_CACHE_HANDLE.compareAndSet(hotArena.getMemory(), (long) nextNodeOffset, expectedValue, newValue);
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