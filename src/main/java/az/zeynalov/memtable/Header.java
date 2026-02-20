package az.zeynalov.memtable;

import java.lang.foreign.MemorySegment;
import java.util.Objects;

public record Header(
   int keySize,
   MemorySegment key,
   int SN,
   byte type){
}
