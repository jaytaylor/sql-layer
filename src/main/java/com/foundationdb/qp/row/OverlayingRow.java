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
import com.foundationdb.server.types3.pvalue.PValue;
import com.foundationdb.server.types3.pvalue.PValueSource;
import com.foundationdb.server.types3.pvalue.PValueTargets;

public final class OverlayingRow extends AbstractRow {
    private final Row underlying;
    private final RowType rowType;
    protected final PValue[] pOverlays;

    public OverlayingRow(Row underlying) {
        this(underlying, underlying.rowType());
    }

    public OverlayingRow(Row underlying, RowType rowType) {
        this.underlying = underlying;
        this.rowType = rowType;
        this.pOverlays = new PValue[underlying.rowType().nFields()];
    }

    public OverlayingRow overlay(int index, PValueSource object) {
        if (checkOverlay(index, object)) {
            PValueTargets.copyFrom(object, pOverlays[index]);
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
            pOverlays[index] = new PValue(underlying.rowType().typeInstanceAt(index));
            return true;
        }
        return true;
    }

    @Override
    public RowType rowType() {
        return rowType;
    }

    @Override
    public PValueSource pvalue(int i) {
        return pOverlays[i] == null ? underlying.pvalue(i) : pOverlays[i];

    }

    @Override
    public HKey hKey() {
        return underlying.hKey();
    }
}
