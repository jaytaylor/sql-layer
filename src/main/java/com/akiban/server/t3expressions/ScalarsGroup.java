
package com.akiban.server.t3expressions;

import com.akiban.server.types3.TClass;
import com.akiban.server.types3.texpressions.TValidatedOverload;

import java.util.Collection;

public interface ScalarsGroup<V extends TValidatedOverload> {
    Collection<? extends V> getOverloads();
    TClass commonTypeAt(int pos);
    boolean hasSameTypeAt(int pos);
}
