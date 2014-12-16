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
package com.foundationdb.qp.storeadapter.indexrow;

import com.foundationdb.ais.model.GroupIndex;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexToHKey;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.row.HKey;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.persistit.Key;
import com.persistit.Value;

public class PersistitGroupIndexRow extends PersistitIndexRow
{
    // Row interface

    @Override
    public HKey ancestorHKey(Table table)
    {
        HKey ancestorHKey = hKeyCache.hKey(table);
        Key hKey = keyCreator.createKey();
        constructHKeyFromIndexKey(hKey, ((GroupIndex)index).indexToHKey(table.getDepth()));
        ancestorHKey.copyFrom(hKey);
        return ancestorHKey;
    }

    // PersistitIndexRow interface

    @Override
    public IndexToHKey indexToHKey()
    {
        return ((GroupIndex)index).indexToHKey(index.leafMostTable().getDepth());
    }

    @Override
    public long tableBitmap()
    {
        return tableBitmap;
    }

    @Override
    public void copyFrom(Key key, Value value)
    {
        super.copyFrom(key, value);
        tableBitmap = value.getLong();
    }

    // PersistitGroupIndexRow interface

    public PersistitGroupIndexRow(StoreAdapter adapter, IndexRowType indexRowType)
    {
        super(adapter, indexRowType);
        Index groupIndex = indexRowType.index();
        assert groupIndex.isGroupIndex();
        index = groupIndex;
    }

    // Object state

    private long tableBitmap;
}
