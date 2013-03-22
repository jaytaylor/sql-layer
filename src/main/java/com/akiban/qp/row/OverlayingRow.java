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

package com.akiban.qp.row;

import com.akiban.qp.rowtype.RowType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.FromObjectValueSource;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.server.types3.Types3Switch;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueSources;
import com.akiban.server.types3.pvalue.PValueTargets;

public final class OverlayingRow extends AbstractRow {
    private final Row underlying;
    private final RowType rowType;
    private final ValueHolder[] overlays;
    protected final PValue[] pOverlays;

    public OverlayingRow(Row underlying) {
        this(underlying, Types3Switch.ON);
    }

    public OverlayingRow(Row underlying, boolean usePValues) {
        this(underlying, underlying.rowType(), usePValues);
    }

    public OverlayingRow(Row underlying, RowType rowType, boolean usePValues) {
        this.underlying = underlying;
        this.rowType = rowType;
        if (usePValues) {
            this.overlays = null;
            this.pOverlays = new PValue[underlying.rowType().nFields()];
        }
        else {
            this.overlays = new ValueHolder[underlying.rowType().nFields()];
            this.pOverlays = null;
        }
    }

    public OverlayingRow overlay(int index, ValueSource object) {
        if (object == null) {
            overlays[index] = null;
        }
        else {
            if (overlays[index] == null)
                overlays[index] = new ValueHolder();
            overlays[index].copyFrom(object);
        }
        return this;
    }

    public OverlayingRow overlay(int index, PValueSource object) {
        if (object == null) {
            pOverlays[index] = null;
        }
        else {
            if (pOverlays[index] == null)
                pOverlays[index] = new PValue(underlying.rowType().typeInstanceAt(index));
            PValueTargets.copyFrom(object,  pOverlays[index]);
        }
        return this;
    }

    public OverlayingRow overlay(int index, Object object) {
        if (pOverlays != null)
            return overlay(index, PValueSources.fromObject(object, underlying.rowType().typeAt(index)).value());
        else
            return overlay(index, valueSource.setExplicitly(object, underlying.rowType().typeAt(index)));
    }

    @Override
    public RowType rowType() {
        return rowType;
    }

    @Override
    public ValueSource eval(int i) {
        return overlays[i] == null ? underlying.eval(i) : overlays[i];
    }

    @Override
    public PValueSource pvalue(int i) {
        return pOverlays[i] == null ? underlying.pvalue(i) : pOverlays[i];

    }

    @Override
    public HKey hKey() {
        return underlying.hKey();
    }

    private final FromObjectValueSource valueSource = new FromObjectValueSource();
}
