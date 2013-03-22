
package com.akiban.server.types3;

import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;

public interface TComparison {
    int compare(TInstance leftInstance, PValueSource left, TInstance rightInstance, PValueSource right);
    void copyComparables(PValueSource source, PValueTarget target);
}
