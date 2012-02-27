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

import com.akiban.ais.model.*;
import com.akiban.server.types.AkType;

import java.util.*;

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
        return akTypes.length;
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
    
    public int declaredFields()
    {
        return index().getKeyColumns().size();
    }

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
        // Types of declared columns
        List<AkType> akTypeList = new ArrayList<AkType>();
        List<IndexColumn> indexColumns = index.getKeyColumns();
        IdentityHashMap<Column, Column> indexColumnMap = new IdentityHashMap<Column, Column>();
        for (int i = 0; i < indexColumns.size(); i++) {
            Column column = indexColumns.get(i).getColumn();
            akTypeList.add(column.getType().akType());
            indexColumnMap.put(column, column);
        }
        // Types of undeclared hkey columns
        for (HKeySegment segment : tableType.hKey().segments()) {
            for (HKeyColumn hKeyColumn : segment.columns()) {
                Column column = hKeyColumn.column();
                if (!indexColumnMap.containsKey(column)) {
                    akTypeList.add(column.getType().akType());
                }
            }
        }
        akTypes = akTypeList.toArray(new AkType[akTypeList.size()]);
    }

    // Object state

    // If index is a GroupIndex, then tableType.userTable() is the leafmost table of the GroupIndex.
    private final UserTableRowType tableType;
    private final Index index;
    private final AkType[] akTypes;
}
