
package com.akiban.util;

public final class ShareHolder<T extends Shareable> {

    // ShareHolder interface

    public T get() {
        return held;
    }

    public void hold(T item) {
        if (isHolding()) {
            held.release();
            held = null;
        }
        assert held == null : held;
        if (item != null) {
            held = item;
            held.acquire();
        }
    }

    public void release() {
        hold(null);
    }

    public boolean isEmpty() {
        return ! isHolding();
    }

    public boolean isHolding() {
        return held != null;
    }

    public boolean isShared() {
        return isHolding() && held.isShared();
    }

    public ShareHolder() {}

    public ShareHolder(T item) {
        hold(item);
    }

    // object interface

    @Override
    public String toString() {
        return held == null ? null : held.toString();
    }

    // object state

    private T held;
}
