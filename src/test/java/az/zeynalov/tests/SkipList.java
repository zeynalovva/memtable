package az.zeynalov.tests;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * A generic Skip List implementation.
 * <p>
 * A skip list is a probabilistic data structure that allows O(log n) average time complexity for
 * search, insertion, and deletion — similar to a balanced BST, but much simpler to implement.
 * <p>
 * Structure: Level 3: head ------> 30 ---------> null Level 2: head --> 10 -> 30 -> 50 -> null
 * Level 1: head --> 10 -> 20 -> 30 -> 40 -> 50 -> null  (base list)
 * <p>
 * Each node is promoted to higher levels with probability P (default 0.5).
 *
 * @param <K> the key type (must be Comparable)
 * @param <V> the value type
 */
public class SkipList<K extends Comparable<K>, V> {

  private static final double DEFAULT_PROBABILITY = 0.5;
  private static final int DEFAULT_MAX_LEVEL = 5;

  // -------------------------------------------------------------------------
  // Inner node class
  // -------------------------------------------------------------------------

  /**
   * A single node in the skip list. Holds a key-value pair and an array of forward pointers — one
   * per level.
   */
  @SuppressWarnings("unchecked")
  private static class Node<K, V> {

    final K key;
    V value;
    final Node<K, V>[] forward; // forward[i] = next node at level i

    Node(K key, V value, int level) {
      this.key = key;
      this.value = value;
      this.forward = new Node[level + 1];
    }
  }

  private final Node<K, V> head;      // sentinel head (key = null)
  private final int maxLevel;  // maximum number of levels
  private final double probability;
  private final Random random;

  private int currentLevel; // highest level currently in use (0-indexed)
  private int size;

  public SkipList() {
    this(DEFAULT_MAX_LEVEL, DEFAULT_PROBABILITY);
  }

  public SkipList(int maxLevel, double probability) {
    if (maxLevel < 1) {
      throw new IllegalArgumentException("maxLevel must be >= 1");
    }
    if (probability <= 0 || probability >= 1) {
      throw new IllegalArgumentException("probability must be in (0, 1)");
    }

    this.maxLevel = maxLevel;
    this.probability = probability;
    this.random = new Random();
    this.currentLevel = 0;
    this.size = 0;
    this.head = new Node<>(null, null, maxLevel);
  }


  public int size() {
    return size;
  }

  public boolean isEmpty() {
    return size == 0;
  }

  public boolean containsKey(K key) {
    return get(key) != null;
  }

  public V get(K key) {
    if (key == null) {
      throw new NullPointerException("key must not be null");
    }

    Node<K, V> current = head;

    // Traverse from the highest level down to level 0
    for (int i = currentLevel; i >= 0; i--) {
      while (current.forward[i] != null &&
          current.forward[i].key.compareTo(key) < 0) {
        current = current.forward[i];
      }
    }

    // At level 0, the next node is the candidate
    current = current.forward[0];
    if (current != null && current.key.compareTo(key) == 0) {
      return current.value;
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  public void put(K key, V value) {
    if (key == null) {
      throw new NullPointerException("key must not be null");
    }

    // update[i] = rightmost node at level i that is to the left of the insertion point
    Node<K, V>[] update = new Node[maxLevel + 1];
    Node<K, V> current = head;

    for (int i = currentLevel; i >= 0; i--) {
      while (current.forward[i] != null &&
          current.forward[i].key.compareTo(key) < 0) {
        current = current.forward[i];
      }
      update[i] = current;
    }


    current = current.forward[0];

    if (current != null && current.key.compareTo(key) == 0) {
      return;
    }

    int newLevel = randomLevel();

    // If the new node introduces levels above the current maximum,
    // point those extra levels at head so they get wired up correctly.
    if (newLevel > currentLevel) {
      for (int i = currentLevel + 1; i <= newLevel; i++) {
        update[i] = head;
      }
      currentLevel = newLevel;
    }

    Node<K, V> newNode = new Node<>(key, value, newLevel);

    // Splice the new node into each level
    for (int i = 0; i <= newLevel; i++) {
      newNode.forward[i] = update[i].forward[i];
      update[i].forward[i] = newNode;
    }

    size++;
  }

  private int randomLevel() {
    int level = 0;
    while (level < maxLevel && random.nextDouble() < probability) {
      level++;
    }
    return level;
  }

}