
package com.akiban.qp.row;

import com.akiban.server.types.ValueSource;
import com.akiban.server.types3.pvalue.PValueSource;

public interface HKey extends Comparable<HKey>
{
    // Object interface
    boolean equals(Object hKey);

    // Comparable interface
    @Override
    int compareTo(HKey o);

    // HKey interface
    boolean prefixOf(HKey hKey);
    int segments();
    void useSegments(int segments);
    void copyTo(HKey target);
    void extendWithOrdinal(int ordinal);
    void extendWithNull();
    ValueSource eval(int i);
    PValueSource pEval(int i);
}
