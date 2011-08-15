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

package com.akiban.qp.row;

import com.akiban.qp.physicaloperator.Bindings;
import com.akiban.server.types.ConversionSource;
import com.akiban.server.types.FromObjectConversionSource;

import java.util.Arrays;

public final class RowValuesHolder {
    // RowValuesHolder interface

    public Object objectAt(int i) {
        return values[i];
    }

    public ConversionSource conversionSourceAt(int i) {
        return sources[i];
    }

    public RowValuesHolder(Object[] values) {
        this.values = new Object[values.length];
        System.arraycopy(values, 0, this.values, 0, this.values.length);
        this.sources = createSources(this.values);
    }

    // Object interface

    @Override
    public String toString() {
        return Arrays.toString(values);
    }


    // for use in this class

    private static FromObjectConversionSource[] createSources(Object[] values) {
        FromObjectConversionSource[] sources = new FromObjectConversionSource[values.length];
        for (int i=0; i < values.length; ++i) {
            FromObjectConversionSource source = new FromObjectConversionSource();
            source.setReflectively(values[i]);
            sources[i] = source;
        }
        return sources;
    }

    // object state

    private final Object[] values;
    private final FromObjectConversionSource[] sources;
}
