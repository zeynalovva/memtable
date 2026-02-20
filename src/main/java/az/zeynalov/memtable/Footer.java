package az.zeynalov.memtable;

import java.lang.foreign.MemorySegment;

public record Footer(int valueSize,
                     MemorySegment value) {
}
