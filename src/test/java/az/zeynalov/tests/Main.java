package az.zeynalov.tests;

public class Main {
  public static void main(String[] args) {
    SkipList<Integer, String> sl = new SkipList<>();

    // Insert
    sl.put(30, "thirty");
    sl.put(10, "ten");
    sl.put(50, "fifty");
    sl.put(20, "twenty");
    sl.put(40, "forty");

    System.out.println("=== After inserting 10,20,30,40,50 ===");
    System.out.println(sl);
    System.out.println("Size: " + sl.size());

    // Search
    System.out.println("\n=== Search ===");
    System.out.println("get(30) = " + sl.get(30));   // thirty
    System.out.println("get(99) = " + sl.get(99));   // null

    // Update
    sl.put(30, "THIRTY-UPDATED");
    System.out.println("\n=== After updating key 30 ===");
    System.out.println("get(30) = " + sl.get(30));   // THIRTY-UPDATED

    // Delete
    System.out.println("\n=== After removing 20 and 50 ===");
    System.out.println(sl);
    System.out.println("Size: " + sl.size());

    // Edge cases
    System.out.println("\n=== Edge Cases ===");
    System.out.println("containsKey(10) = " + sl.containsKey(10)); // true
    System.out.println("containsKey(50) = " + sl.containsKey(50)); // false

    // Larger insertion test
    System.out.println("\n=== Large Insertion (1-100) ===");
    SkipList<Integer, Integer> big = new SkipList<>();
    for (int i = 1; i <= 100; i++) {
      big.put(i, i * i);
    }

    System.out.println("Size: " + big.size());
    System.out.println("get(7)  = " + big.get(7));   // 49
    System.out.println("get(10) = " + big.get(10));  // 100
    System.out.println("get(50) = " + big.get(50));  // 2500


    System.out.println("After removing odd keys — size: " + big.size()); // 50
    System.out.println("get(4)  = " + big.get(4));   // 16
    System.out.println("get(5)  = " + big.get(5));   // null (removed)
  }
}
