
package com.akiban.server.types.typestests;

import com.akiban.server.types.AkType;

import java.util.Collections;
import java.util.Set;

public abstract class SimpleLinkedConversion implements LinkedConversion<Void> {

    @Override
    public final void checkPut(Void expected) {
    }

    @Override
    public final void syncConversions() {
    }

    @Override
    public Set<? extends AkType> unsupportedTypes() {
        return Collections.emptySet();
    }
}
