
package com.akiban.util;

public interface Shareable {
    void acquire();
    boolean isShared();
    void release();
}
