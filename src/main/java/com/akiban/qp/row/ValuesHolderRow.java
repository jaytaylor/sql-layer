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

import com.akiban.qp.rowtype.AggregatedRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;

import java.util.ArrayList;
import java.util.List;

public class ValuesHolderRow extends AbstractRow {

    public void clear() {
        for (ValueHolder value : values) {
            value.clear();
        }
    }

    public ValueHolder holderAt(int index) {
        return values.get(index);
    }

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

    public ValuesHolderRow(AggregatedRowType rowType) {
        this.rowType = rowType;
        values = new ArrayList<ValueHolder>();
        for (int i=0; i < rowType.nFields(); ++i) {
            values.add(new ValueHolder());
        }
    }

    private final RowType rowType;
    private final List<ValueHolder> values;
}
