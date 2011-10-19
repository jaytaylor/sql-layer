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

import com.akiban.qp.rowtype.RowType;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;

import java.util.ArrayList;
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
    public ValueSource eval(int i) {
        ValueHolder value = values.get(i);
        if (!value.hasSourceState()) {
            throw new IllegalStateException("value at index " + i + " was never set");
        }
        return value;
    }

    @Override
    public void acquire() {
        if (isMutable)
            super.acquire();
    }

    @Override
    public void release() {
        if (isMutable)
            super.release();
    }

    @Override
    public boolean isShared() {
        return isMutable && super.isShared();
    }

    // for use by subclasses

    AbstractValuesHolderRow(RowType rowType, boolean isMutable) {
        this.isMutable = isMutable;
        this.rowType = rowType;
        values = new ArrayList<ValueHolder>();
        for (int i=0; i < rowType.nFields(); ++i) {
            values.add(new ValueHolder());
        }
    }

    AbstractValuesHolderRow(RowType rowType, boolean isMutable, Iterator<? extends ValueSource> initialValues) {
        this(rowType, isMutable);
        int i = 0;
        while(initialValues.hasNext()) {
            if (i >= values.size())
                throw new IllegalArgumentException("too many initial values: reached limit of " + values.size());
            ValueSource nextValue = initialValues.next();
            AkType nextValueType = nextValue.getConversionType();
            if (nextValueType != AkType.NULL && nextValueType != rowType.typeAt(i))
                throw new IllegalArgumentException(
                        "value at index " + i + " expected type " + rowType.typeAt(i)
                                + ", was " + nextValueType + ": " + nextValue);
            values.get(i++).copyFrom(nextValue);
        }
        if (i != values.size())
            throw new IllegalArgumentException("not enough initial values: required " + values.size() + " but saw " + i);
    }

    void clear() {
        checkMutable();
        for (ValueHolder value : values) {
            value.clear();
        }
    }

    ValueHolder holderAt(int index) {
        checkMutable();
        return values.get(index);
    }

    private void checkMutable() {
        if (!isMutable)
            throw new IllegalStateException("can't invoke method on an immutable AbstractValuesHolderRow");
    }

    private final RowType rowType;
    private final List<ValueHolder> values;
    private final boolean isMutable;
}
