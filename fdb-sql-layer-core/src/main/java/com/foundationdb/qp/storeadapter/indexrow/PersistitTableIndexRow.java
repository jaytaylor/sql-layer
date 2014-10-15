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

import com.foundationdb.ais.model.IndexToHKey;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.storeadapter.PersistitHKey;
import com.foundationdb.qp.row.HKey;
import com.foundationdb.qp.rowtype.IndexRowType;

public class PersistitTableIndexRow extends PersistitIndexRow
{
    // Row interface

    @Override
    public HKey ancestorHKey(Table table)
    {
        PersistitHKey ancestorHKey;
        PersistitHKey leafmostHKey = hKeyCache.hKey(leafmostTable);
        if (table == leafmostTable) {
            ancestorHKey = leafmostHKey;
        } else {
            ancestorHKey = hKeyCache.hKey(table);
            leafmostHKey.copyTo(ancestorHKey);
            ancestorHKey.useSegments(table.getDepth() + 1);
        }
        return ancestorHKey;
    }

    // PersistitIndexRow interface

    @Override
    public IndexToHKey indexToHKey()
    {
        return index.indexToHKey();
    }

    // PersistitTableIndexRow interface

    public PersistitTableIndexRow(StoreAdapter adapter, IndexRowType indexRowType)
    {
        super(adapter, indexRowType);
        this.index = (TableIndex) indexRowType.index();
    }

    // Object state

    private final TableIndex index;
}
