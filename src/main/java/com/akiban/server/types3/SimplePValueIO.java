
package com.akiban.server.types3;

import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;

public abstract class SimplePValueIO implements PValueIO {

    protected abstract void copy(PValueSource in, TInstance typeInstance, PValueTarget out);

    @Override
    public void copyCanonical(PValueSource in, TInstance typeInstance, PValueTarget out) {
        copy(in, typeInstance, out);
    }

    @Override
    public void writeCollating(PValueSource in, TInstance typeInstance, PValueTarget out) {
        copy(in, typeInstance, out);
    }

    @Override
    public void readCollating(PValueSource in, TInstance typeInstance, PValueTarget out) {
        copy(in, typeInstance, out);
    }
}
