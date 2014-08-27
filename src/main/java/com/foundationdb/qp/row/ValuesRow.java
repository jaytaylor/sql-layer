/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;

import java.util.Arrays;

public final class ValuesRow extends AbstractRow {
    @Override
    public RowType rowType() {
        return rowType;
    }

    @Override
    public ValueSource value(int i) {
        return values[i];
    }

    @Override
    public HKey hKey() {
        return null;
    }

    @Override
    public boolean isBindingsSensitive() {
        return false;
    }

    public ValuesRow(RowType rowType, ValueSource... values) {
        this.rowType = rowType;
        this.values = values;
        if (rowType.nFields() != values.length) {
            throw new IllegalArgumentException(
                    "row type " + rowType + " requires " + rowType.nFields() + " fields, but "
                            + values.length + " values given: " + Arrays.asList(values));
        }
        for (int i = 0, max = values.length; i < max; ++i) {
            TClass requiredType = rowType.typeAt(i).typeClass();
            TClass actualType = TInstance.tClass(values[i].getType());
            if (requiredType != actualType)
                throw new IllegalArgumentException("value " + i + " should be " + requiredType
                        + " but was " + actualType);
        }
    }

    public ValuesRow(RowType rowType, Object... objects) {
        this.rowType = rowType;
        values = new ValueSource[rowType.nFields()];
        
        if (rowType.nFields() != objects.length) {
            throw new IllegalArgumentException(
                    "row type " + rowType + " requires " + rowType.nFields() + " fields, but "
                            + objects.length + " values given: " + Arrays.asList(objects));
        }
        for (int i = 0; i < values.length; ++i) {
            values[i] = ValueSources.valuefromObject(objects[i], rowType.typeAt(i));
        }
    }
    
    private final RowType rowType;
    private final ValueSource[] values;
}
