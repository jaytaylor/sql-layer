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
import com.foundationdb.server.types3.TInstance;
import com.foundationdb.server.types3.pvalue.PValue;
import com.foundationdb.server.types3.pvalue.PValueSource;
import com.foundationdb.server.types3.pvalue.PValueTargets;

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
    public PValueSource pvalue(int i) {
        PValue value = pValues.get(i);
        if (!value.hasAnyValue())
            throw new IllegalStateException("value at index " + i + " was never set");
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
        int nfields = rowType.nFields();
        pValues = new ArrayList<>(nfields);
        for (int i = 0; i < nfields; ++i) {
            TInstance tinst = rowType.typeInstanceAt(i);
            pValues.add(new PValue(tinst));
        }
    }

    AbstractValuesHolderRow(RowType rowType, List<PValue> values) {
        this.isMutable = false;
        this.rowType = rowType;
        this.pValues = Collections.unmodifiableList(values);
    }

    AbstractValuesHolderRow(RowType rowType, boolean isMutable,
                            Iterator<? extends PValueSource> initialPValues)
    {
        this(rowType, isMutable);
        int i = 0;
        while(initialPValues.hasNext()) {
            if (i >= pValues.size())
                throw new IllegalArgumentException("too many initial values: reached limit of " + pValues.size());
            PValueSource nextValue = initialPValues.next();
            TInstance nextValueType = nextValue.tInstance();
            TInstance expectedTInst = rowType.typeInstanceAt(i);
            if (TInstance.tClass(nextValueType) != TInstance.tClass(expectedTInst))
                throw new IllegalArgumentException(
                        "value at index " + i + " expected type " + rowType.typeInstanceAt(i)
                                + ", but PUnderlying was " + nextValueType + ": " + nextValue);
            PValueTargets.copyFrom(nextValue, pValues.get(i++));
        }
        if (i != pValues.size())
            throw new IllegalArgumentException("not enough initial values: required " + pValues.size() + " but saw " + i);
    }

    void clear() {
        checkMutable();
    }

    PValue pvalueAt(int index) {
        checkMutable();
        return pValues.get(index);
    }

    private void checkMutable() {
        if (!isMutable)
            throw new IllegalStateException("can't invoke method on an immutable AbstractValuesHolderRow");
    }

    private final RowType rowType;
    protected final List<PValue> pValues;
    private final boolean isMutable;
}
