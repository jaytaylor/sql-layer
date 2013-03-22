
package com.akiban.server.types.util;

import com.akiban.server.types.AbstractValueSource;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueSourceIsNullException;

public final class BoolValueSource extends AbstractValueSource {

    // BoolValueSource class interface

    public static final ValueSource OF_TRUE = new BoolValueSource(true);
    public static final ValueSource OF_FALSE = new BoolValueSource(false);
    public static final ValueSource OF_NULL = new BoolValueSource(null);

    public static ValueSource of(Boolean value) {
        return value == null ? OF_NULL : of(value.booleanValue());
    }

    public static ValueSource of(boolean value) {
        return value ? OF_TRUE : OF_FALSE;
    }

    // ValueSource interface

    @Override
    public boolean getBool() {
        if (value == null)
            throw new ValueSourceIsNullException();
        return value;
    }

    @Override
    public boolean isNull() {
        return value == null;
    }

    @Override
    public AkType getConversionType() {
        return AkType.BOOL;
    }

    // object interface

    @Override
    public String toString() {
        return toString;
    }

    // for use in this class

    private BoolValueSource(Boolean value) {
        this.value = value;
        this.toString = "BoolValueSource(" + value + ")";
    }

    // object state
    private final Boolean value;
    private final String toString;
}
