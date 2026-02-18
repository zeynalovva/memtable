package az.zeynalov.memtable;

public record Pair<F, S>(F value, S numberOfBytes) {
    public static <F, S> Pair<F, S> of(F value, S numberOfBytes) {
        return new Pair<>(value, numberOfBytes);
    }
}