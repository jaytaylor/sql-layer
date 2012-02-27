/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.types.util;

import com.akiban.server.types.AbstractValueSource;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueSourceIsNullException;

public final class IntValueSource extends AbstractValueSource {

    // IntValueSource class interface

    public static final ValueSource OF_NULL = new IntValueSource(null);

    public static ValueSource of(Integer value) {
        return value == null ? OF_NULL : of(value.intValue());
    }

    public static ValueSource of(int value) {
        return new IntValueSource(value);
    }

    // ValueSource interface

    @Override
    public long getInt() {
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
        return AkType.INT;
    }

    // object interface

    @Override
    public String toString() {
        return toString;
    }

    // for use in this class

    private IntValueSource(Integer value) {
        this.value = value;
        this.toString = "IntValueSource(" + value + ")";
    }

    // object state
    private final Integer value;
    private final String toString;
}
