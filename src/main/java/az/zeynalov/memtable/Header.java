package az.zeynalov.memtable;

import java.lang.foreign.MemorySegment;

public record Header(
   int keySize,
   MemorySegment key,
   long SN,
   byte type){
}
