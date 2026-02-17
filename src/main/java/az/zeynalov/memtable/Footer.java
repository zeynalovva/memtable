package az.zeynalov.memtable;

public record Footer(int valueSize,
                     byte[] value) {
}
