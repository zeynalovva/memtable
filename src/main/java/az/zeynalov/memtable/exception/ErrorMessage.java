package az.zeynalov.memtable.exception;

public class ErrorMessage {

  public final static String ARENA_IS_FULL = "No memory left in arena!";
  public final static String ARENA_LARGE_VARINT = "Varint is too large to be read from arena!";
  public final static String ARENA_SIZE_MISMATCH = "Size of payload does not match the expected size!";

}
