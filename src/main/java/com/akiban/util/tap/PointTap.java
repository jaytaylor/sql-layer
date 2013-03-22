
package com.akiban.util.tap;

public class PointTap
{

    public void hit() {
        internal.in();
        internal.out();
    }

    PointTap(Tap internal) {
        this.internal = internal;
    }

    private final Tap internal;
}
