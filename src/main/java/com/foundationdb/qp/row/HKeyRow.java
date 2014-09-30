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

import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.rowtype.HKeyRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.util.HKeyCache;
import com.foundationdb.server.types.value.ValueSource;

public class HKeyRow extends AbstractRow
{
    // Object interface

    @Override
    public String toString()
    {
        return hKey.toString();
    }

    // Row interface

    @Override
    public RowType rowType()
    {
        return rowType;
    }

    @Override
    public ValueSource uncheckedValue(int i) {
        return hKey.pEval(i);
    }

    @Override
    public HKey hKey()
    {
        return hKey;
    }

    @Override
    public HKey ancestorHKey(Table table)
    {
        // TODO: This does the wrong thing for hkeys derived from group index rows!
        // TODO: See bug 997746.
        HKey ancestorHKey = hKeyCache.hKey(table);
        hKey.copyTo(ancestorHKey);
        ancestorHKey.useSegments(table.getDepth() + 1);
        return ancestorHKey;
    }

    @Override
    public Row subRow(RowType subRowType)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isBindingsSensitive() {
        return false;
    }

    // HKeyRow interface

    public HKeyRow(HKeyRowType rowType, HKey hKey, HKeyCache<HKey> hKeyCache)
    {
        this.hKeyCache = hKeyCache;
        this.rowType = rowType;
        this.hKey = hKey;
    }
    
    // Object state

    private final HKeyCache<HKey> hKeyCache;
    private final HKeyRowType rowType;
    private HKey hKey;
}
