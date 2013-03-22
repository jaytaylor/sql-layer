
package com.akiban.qp.operator;

import com.akiban.qp.row.RowBase;

public final class NoLimit implements Limit {

    private static final Limit INSTANCE = new NoLimit();

    public static Limit instance() {
        return INSTANCE;
    }

    private NoLimit() {
        // private ctor
    }

    @Override
    public boolean limitReached(RowBase row) {
        return false;
    }

    @Override
    public String toString() {
        return "NO_LIMIT";
    }
}
