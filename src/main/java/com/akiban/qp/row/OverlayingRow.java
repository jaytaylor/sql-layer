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
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.FromObjectValueSource;
import com.akiban.util.Undef;

public final class OverlayingRow extends AbstractRow {
    private final Row underlying;
    private final Object[] overlays;

    public OverlayingRow(Row underlying) {
        this.underlying = underlying;
        this.overlays = new Object[underlying.rowType().nFields()];
        for (int i=0; i < overlays.length; ++i) {
            overlays[i] = Undef.only();
        }
    }
    public OverlayingRow overlay(int index, Object object) {
        overlays[index] = object;
        return this;
    }

    @Override
    public RowType rowType() {
        return underlying.rowType();
    }

    @Override
    public ValueSource bindSource(int i, Bindings bindings) {
        if (Undef.isUndefined(overlays[i])) {
            return underlying.bindSource(i, bindings);
        } else {
            valueSource.setReflectively(overlays[i]);
            return valueSource;
        }
    }

    @Override
    public HKey hKey() {
        return underlying.hKey();
    }

    private final FromObjectValueSource valueSource = new FromObjectValueSource();
}
