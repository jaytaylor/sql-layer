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

import com.akiban.ais.model.UserTable;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.FromObjectValueSource;
import com.akiban.server.types.util.ValueHolder;

public final class OverlayingRow extends AbstractRow {
    private final Row underlying;
    private final ValueHolder[] overlays;

    public OverlayingRow(Row underlying) {
        this.underlying = underlying;
        this.overlays = new ValueHolder[underlying.rowType().nFields()];
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

    public OverlayingRow overlay(int index, Object object) {
        return overlay(index, valueSource.setExplicitly(object, underlying.rowType().typeAt(index)));
    }

    @Override
    public boolean containsRealRowOf(UserTable userTable) {
        return underlying.containsRealRowOf(userTable);
    }

    @Override
    public RowType rowType() {
        return underlying.rowType();
    }

    @Override
    public ValueSource eval(int i) {
        return overlays[i] == null ? underlying.eval(i) : overlays[i];
    }

    @Override
    public HKey hKey() {
        return underlying.hKey();
    }

    private final FromObjectValueSource valueSource = new FromObjectValueSource();
}
