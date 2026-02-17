package az.zeynalov.memtable;

import java.nio.ByteBuffer;

public record Header(
   int keySize,
   ByteBuffer key,
   int SN){

}
