
package com.akiban.server.types3;

public interface LazyList<T> extends Iterable<T> {
    public T get(int i);
    public int size();
}
