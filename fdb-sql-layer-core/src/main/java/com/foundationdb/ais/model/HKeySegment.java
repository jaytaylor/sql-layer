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

package com.foundationdb.ais.model;

import java.util.ArrayList;
import java.util.List;

public class HKeySegment
{
    @Override
    public String toString()
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append(table().getName().getTableName());
        buffer.append(": (");
        boolean firstColumn = true;
        for (HKeyColumn hKeyColumn : columns()) {
            if (firstColumn) {
                firstColumn = false;
            } else {
                buffer.append(", ");
            }
            buffer.append(hKeyColumn.toString());
        }
        buffer.append(")");
        return buffer.toString();
    }

    public HKeySegment(HKey hKey, Table table)
    {
        this.hKey = hKey;
        this.table = table;
        if (hKey.segments().isEmpty()) {
            this.positionInHKey = 0;
        } else {
            HKeySegment lastSegment = hKey.segments().get(hKey.segments().size() - 1);
            this.positionInHKey =
                lastSegment.columns().isEmpty()
                ? lastSegment.positionInHKey() + 1
                : lastSegment.columns().get(lastSegment.columns().size() - 1).positionInHKey() + 1;
        }
    }
    
    public HKey hKey()
    {
        return hKey;
    }

    public Table table()
    {
        return table;
    }

    public int positionInHKey()
    {
        return positionInHKey;
    }

    public List<HKeyColumn> columns()
    {
        return columns;
    }

    public HKeyColumn addColumn(Column column)
    {
        assert column != null;
        HKeyColumn hKeyColumn = new HKeyColumn(this, column);
        columns.add(hKeyColumn);
        return hKeyColumn;
    }

    private final HKey hKey;
    private final Table table;
    private final List<HKeyColumn> columns = new ArrayList<>();
    private final int positionInHKey;
}
