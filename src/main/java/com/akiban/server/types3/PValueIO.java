
package com.akiban.server.types3;

import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;

public interface PValueIO {
    void copyCanonical(PValueSource in, TInstance typeInstance, PValueTarget out);
    void writeCollating(PValueSource in, TInstance typeInstance, PValueTarget out);
    void readCollating(PValueSource in, TInstance typeInstance, PValueTarget out);
}
