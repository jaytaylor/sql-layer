
package com.akiban.server.types3;

public final class ReversedLazyList<T> extends LazyListBase<T> {

    @Override
    public T get(int i) {
        i = originalSize -i - 1;
        return original.get(i);
    }

    @Override
    public int size() {
        return originalSize;
    }

    public ReversedLazyList(LazyList<? extends T> original) {
        this.original = original;
        originalSize = original.size();
    }

    private final LazyList<? extends T> original;
    private final int originalSize;
}
