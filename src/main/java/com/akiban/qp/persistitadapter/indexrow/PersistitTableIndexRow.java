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
package com.akiban.qp.persistitadapter.indexrow;

import com.akiban.ais.model.IndexToHKey;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.persistitadapter.PersistitHKey;
import com.akiban.qp.row.HKey;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.server.service.tree.KeyCreator;

public class PersistitTableIndexRow extends PersistitIndexRow
{
    // RowBase interface

    @Override
    public HKey ancestorHKey(UserTable table)
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

    public PersistitTableIndexRow(KeyCreator keyCreator, StoreAdapter adapter, IndexRowType indexRowType)
    {
        super(keyCreator, adapter, indexRowType);
        this.index = (TableIndex) indexRowType.index();
    }

    // Object state

    private final TableIndex index;
}
