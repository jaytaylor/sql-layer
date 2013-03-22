
package com.akiban.server.types3;

import java.util.Collection;
import java.util.Collections;

public final class TCommutativeOverloads {

    public static TCommutativeOverloads createFrom(TOverload... overloads) {
        return new TCommutativeOverloads(overloads);
    }

    public void addTo(Collection<? super TOverload> overloadsSet) {
        Collections.addAll(overloadsSet, origins);
    }

    private TCommutativeOverloads(TOverload[] origins) {
        this.origins = origins;
    }

    private final TOverload[] origins;
}
