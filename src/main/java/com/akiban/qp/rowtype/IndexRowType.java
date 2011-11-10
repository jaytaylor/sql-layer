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

package com.akiban.qp.rowtype;

import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.HKey;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.server.types.AkType;

import java.util.List;

public class IndexRowType extends AisRowType
{
    // Object interface

    @Override
    public String toString()
    {
        return index.toString();
    }

    // RowType interface

    @Override
    public int nFields()
    {
        return index.getColumns().size();
    }

    @Override
    public AkType typeAt(int index)
    {
        return akTypes[index];
    }

    @Override
    public HKey hKey()
    {
        return tableType.hKey();
    }

    // IndexRowType interface

    public UserTableRowType tableType()
    {
        return tableType;
    }

    public Index index()
    {
        return index;
    }

    public IndexRowType(Schema schema, UserTableRowType tableType, Index index)
    {
        super(schema, schema.nextTypeId());
        if (index.isGroupIndex()) {
            GroupIndex groupIndex = (GroupIndex) index;
            assert groupIndex.leafMostTable() == tableType.userTable();
        }
        this.tableType = tableType;
        this.index = index;
        List<IndexColumn> indexColumns = index.getColumns();
        akTypes = new AkType[indexColumns.size()];
        for (int i = 0; i < indexColumns.size(); i++) {
            akTypes[i] = indexColumns.get(i).getColumn().getType().akType();
        }
    }

    // Object state

    // If index is a GroupIndex, then tableType.userTable() is the leafmost table of the GroupIndex.
    private final UserTableRowType tableType;
    private final Index index;
    private final AkType[] akTypes;
}
