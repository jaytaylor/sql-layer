/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.qp.row;

import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.types.FromObjectValueSource;

import java.util.Arrays;

public final class RowValuesHolder {
    // RowValuesHolder interface

    public Object objectAt(int i) {
        return values[i];
    }

    public ValueSource valueSourceAt(int i) {
        return sources[i];
    }

    public RowValuesHolder(Object[] values) {
        this(values, null);
    }

    public RowValuesHolder(Object[] values, AkType[] explicitTypes) {
        this.values = new Object[values.length];
        System.arraycopy(values, 0, this.values, 0, this.values.length);
        this.sources = createSources(this.values, explicitTypes);
    }

    // Object interface

    @Override
    public String toString() {
        return Arrays.toString(values);
    }


    // for use in this class

    private static FromObjectValueSource[] createSources(Object[] values, AkType[] explicitTypes) {
        FromObjectValueSource[] sources = new FromObjectValueSource[values.length];
        for (int i=0; i < values.length; ++i) {
            FromObjectValueSource source = new FromObjectValueSource();
            if(explicitTypes == null || explicitTypes[i] == null)
                source.setReflectively(values[i]);
            else
                source.setExplicitly(values[i], explicitTypes[i]);
            sources[i] = source;
        }
        return sources;
    }

    // object state

    private final Object[] values;
    private final FromObjectValueSource[] sources;
}
