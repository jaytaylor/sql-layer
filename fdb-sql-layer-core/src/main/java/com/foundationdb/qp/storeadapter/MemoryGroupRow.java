/**
 * Copyright (C) 2009-2015 FoundationDB, LLC
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

package com.foundationdb.qp.storeadapter;

import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.row.AbstractRow;
import com.foundationdb.qp.row.HKey;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.service.tree.KeyCreator;
import com.foundationdb.server.types.value.ValueSource;
import com.persistit.Key;

public class MemoryGroupRow extends AbstractRow
{
    private final KeyCreator keyCreator;
    private final Row underlying;
    private final HKey currentHKey;

    public MemoryGroupRow(KeyCreator keyCreator, Row abstractRow, Key key) {
        this.keyCreator = keyCreator;
        this.underlying = abstractRow;
        this.currentHKey = keyCreator.newHKey(rowType().table().hKey());
        if(key != null) {
            currentHKey.copyFrom(key);
        }
    }

    @Override
    public HKey hKey()  {
        return currentHKey;
    }

    @Override
    public HKey ancestorHKey(Table table)  {
        HKey ancestorHKey = keyCreator.newHKey(table.hKey());
        currentHKey.copyTo(ancestorHKey);
        ancestorHKey.useSegments(table.getDepth() + 1);
        return ancestorHKey;
    }

    @Override
    public boolean containsRealRowOf(Table table) {
        return rowType().table() == table;
    }

    @Override
    public boolean isBindingsSensitive() {
        return false;
    }

    @Override
    public RowType rowType() {
        return underlying.rowType();
    }

    @Override
    protected ValueSource uncheckedValue(int i) {
        return underlying.value(i);
    }
}

