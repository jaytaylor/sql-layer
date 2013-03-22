
package com.akiban.server.types.extract;

import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;

public abstract class ObjectExtractor<T> extends AbstractExtractor {

    public abstract T getObject(ValueSource source);
    public abstract T getObject(String string);

    public String asString(T value) {
        return String.valueOf(value);
    }

    // package-private ctor
    ObjectExtractor(AkType type) {
        super(type);
    }
}
