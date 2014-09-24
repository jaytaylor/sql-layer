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
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTargets;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

class AbstractValuesHolderRow extends AbstractRow {

    @Override
    public RowType rowType() {
        return rowType;
    }

    @Override
    public HKey hKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ValueSource uncheckedValue(int i) {
        Value value = values.get(i);
        if (!value.hasAnyValue())
            throw new IllegalStateException("value at index " + i + " was never set");
        return value;
    }

    // for use by subclasses

    AbstractValuesHolderRow(RowType rowType, boolean isMutable) {
        this.isMutable = isMutable;
        this.rowType = rowType;
        int nfields = rowType.nFields();
        values = new ArrayList<>(nfields);
        for (int i = 0; i < nfields; ++i) {
            TInstance type = rowType.typeAt(i);
            values.add(new Value(type));
        }
    }

    AbstractValuesHolderRow(RowType rowType, List<Value> values) {
        this.isMutable = false;
        this.rowType = rowType;
        this.values = Collections.unmodifiableList(values);
        checkTypes();
    }

    AbstractValuesHolderRow(RowType rowType, boolean isMutable,
                            Iterator<? extends ValueSource> initialValues)
    {
        this(rowType, isMutable);
        int i = 0;
        while(initialValues.hasNext()) {
            if (i >= values.size())
                throw new IllegalArgumentException("too many initial values: reached limit of " + values.size());
            ValueSource nextValue = initialValues.next();
            TInstance nextValueType = nextValue.getType();
            TInstance expectedTInst = rowType.typeAt(i);
            if (TInstance.tClass(nextValueType) != TInstance.tClass(expectedTInst))
                // TODO: nextValue shouldn't have a null type, it should match the TInstance type instead
                throw new IllegalArgumentException(
                        "value at index " + i + " expected type " + rowType.typeAt(i)
                                + ", but UnderlyingType was " + nextValueType + ": " + nextValue);
            ValueTargets.copyFrom(nextValue, values.get(i++));
        }
        if (i != values.size())
            throw new IllegalArgumentException("not enough initial values: required " + values.size() + " but saw " + i);
    }

    void clear() {
        checkMutable();
    }

    Value valueAt(int index) {
        checkMutable();
        return values.get(index);
    }

    private void checkMutable() {
        if (!isMutable)
            throw new IllegalStateException("can't invoke method on an immutable AbstractValuesHolderRow");
    }

    @Override
    public boolean isBindingsSensitive() {
        return false;
    }

    private final RowType rowType;
    protected final List<Value> values;
    private final boolean isMutable;
}
