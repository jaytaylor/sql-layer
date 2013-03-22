
package com.akiban.server.types3;

import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;

public abstract class SymmetricPValueIO implements PValueIO {

    protected abstract void symmetricCollationCopy(PValueSource in, TInstance typeInstance, PValueTarget out);

    @Override
    public void writeCollating(PValueSource in, TInstance typeInstance, PValueTarget out) {
        symmetricCollationCopy(in, typeInstance, out);
    }

    @Override
    public void readCollating(PValueSource in, TInstance typeInstance, PValueTarget out) {
        symmetricCollationCopy(in, typeInstance, out);
    }
}
