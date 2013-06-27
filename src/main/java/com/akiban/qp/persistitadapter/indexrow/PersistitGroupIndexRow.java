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

import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.IndexToHKey;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.persistitadapter.PersistitHKey;
import com.akiban.qp.row.HKey;
import com.akiban.qp.rowtype.IndexRowType;
import com.persistit.Exchange;
import com.persistit.exception.PersistitException;

public class PersistitGroupIndexRow extends PersistitIndexRow
{
    // RowBase interface

    @Override
    public HKey ancestorHKey(UserTable table)
    {
        PersistitHKey ancestorHKey = hKeyCache.hKey(table);
        constructHKeyFromIndexKey(ancestorHKey.key(), index.indexToHKey(table.getDepth()));
        return ancestorHKey;
    }

    // PersistitIndexRow interface

    public IndexToHKey indexToHKey()
    {
        return index.indexToHKey(index.leafMostTable().getDepth());
    }

    public long tableBitmap()
    {
        return tableBitmap;
    }

    public void copyFromExchange(Exchange exchange)
    {
        super.copyFromExchange(exchange);
        tableBitmap = exchange.getValue().getLong();
    }

    // PersistitGroupIndexRow interface

    public PersistitGroupIndexRow(PersistitAdapter adapter, IndexRowType indexRowType)
    {
        super(adapter, indexRowType);
        this.index = (GroupIndex) indexRowType.index();
    }

    // Object state

    private final GroupIndex index;
    private long tableBitmap;
}
