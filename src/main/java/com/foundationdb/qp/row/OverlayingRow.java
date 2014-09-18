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
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTargets;

public final class OverlayingRow extends AbstractRow {
    private final Row underlying;
    private final RowType rowType;
    protected final Value[] pOverlays;

    public OverlayingRow(Row underlying) {
        this(underlying, underlying.rowType());
    }

    public OverlayingRow(Row underlying, RowType rowType) {
        this.underlying = underlying;
        this.rowType = rowType;
        this.pOverlays = new Value[underlying.rowType().nFields()];
        // this might cause problems if it is later overlayed
        checkTypes();
    }

    public OverlayingRow overlay(int index, ValueSource object) {
        if (checkOverlay(index, object)) {
            ValueTargets.copyFrom(object, pOverlays[index]);
        }
        return this;
    }

    public OverlayingRow overlay(int index, Object object) {
        if (checkOverlay(index, object)) {
            pOverlays[index].putObject(object);
        }
        return this;
    }
    
    private boolean  checkOverlay (int index, Object object) {
        if (object == null) {
            pOverlays[index] = null;
            return false;
        } else if (pOverlays[index] == null) {
            pOverlays[index] = new Value(underlying.rowType().typeAt(index));
            return true;
        }
        return true;
    }

    @Override
    public RowType rowType() {
        return rowType;
    }

    @Override
    public ValueSource uncheckedValue(int i) {
        return pOverlays[i] == null ? underlying.value(i) : pOverlays[i];

    }

    @Override
    public HKey hKey() {
        return underlying.hKey();
    }

    @Override
    public boolean isBindingsSensitive() {
        return underlying.isBindingsSensitive();
    }
}
