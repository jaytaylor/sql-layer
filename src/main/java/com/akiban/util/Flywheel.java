
package com.akiban.util;

import java.util.ArrayDeque;
import java.util.Queue;

public abstract class Flywheel<T> implements Recycler<T> {

    @Override
    public String toString() {
        return String.format("%d in reserve, %d created",
                reserves == null ? 0 : reserves.size(),
                created
        );
    }

    protected abstract T createNew();
    
    protected int defaultCapacity() {
        return 16;
    }
    
    public T get() {
        if (reserves == null || reserves.isEmpty()) {
            ++created;
            return createNew();
        }
        return reserves.poll();
    }
    
    public void recycle(T element) {
        if (reserves == null)
            reserves = new ArrayDeque<>(defaultCapacity());
        reserves.offer(element);
    }

    private Queue<T> reserves;
    private int created;
}
