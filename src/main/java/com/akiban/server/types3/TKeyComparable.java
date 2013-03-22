
package com.akiban.server.types3;

public final class TKeyComparable {

    public TClass getLeftTClass() {
        return leftTClass;
    }

    public TClass getRightTClass() {
        return rightTClass;
    }

    public TComparison getComparison() {
        return comparison;
    }

    public TKeyComparable(TClass leftClass, TClass rightClass, TComparison comparison) {
        this.leftTClass = leftClass;
        this.rightTClass = rightClass;
        this.comparison = comparison;
    }

    private final TClass leftTClass;
    private final TClass rightTClass;
    private final TComparison comparison;
}
