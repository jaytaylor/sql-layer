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

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.UserTable;
import com.akiban.server.RowDef;

import java.util.ArrayList;

import static java.lang.Math.max;

// UserTable RowTypes are indexed by the UserTable's RowDef's ordinal. Derived RowTypes get higher values.

public class Schema
{
    public Schema(AkibanInformationSchema ais)
    {
        this.ais = ais;
        this.typeIdCounter = -1;
        // User tables: use ordinal as typeId
        for (UserTable userTable : ais.getUserTables().values()) {
            RowDef rowDef = (RowDef) userTable.rowDef();
            int ordinal = rowDef.getOrdinal();
            UserTableRowType userTableRowType = new UserTableRowType(this, userTable);
            setRowType(ordinal, userTableRowType);
            this.typeIdCounter = max(this.typeIdCounter, ordinal);
            // Indexes
            this.typeIdCounter++;
            for (Index index : userTable.getIndexesIncludingInternal()) {
                IndexRowType indexRowType = new IndexRowType(this, index);
                userTableRowType.addIndexRowType(indexRowType);
                setRowType(indexRowType.typeId(), indexRowType);
            }
        }
        this.typeIdCounter++;
    }

    public AkibanInformationSchema ais()
    {
        return ais;
    }

    public synchronized UserTableRowType userTableRowType(UserTable table)
    {
        RowDef rowDef = (RowDef) table.rowDef();
        return (UserTableRowType) rowTypes.get(rowDef.getOrdinal());
    }

    public synchronized IndexRowType indexRowType(Index index)
    {
        assert index.getTable().isUserTable() : index;
        return userTableRowType((UserTable) index.getTable()).indexRowType(index);
    }

    public synchronized FlattenedRowType newFlattenType(RowType parent, RowType child)
    {
        return new FlattenedRowType(this, typeIdCounter++, parent, child);
    }

    public int maxTypeId()
    {
        return rowTypes.size() - 1;
    }

    // For use by this package

    synchronized int nextTypeId()
    {
        return typeIdCounter++;
    }

    // For use by this class

    private void setRowType(int typeId, RowType rowType)
    {
        int requiredEntries = typeId + 1;
        while (rowTypes.size() < requiredEntries) {
            rowTypes.add(null);
        }
        rowTypes.set(typeId, rowType);
    }

    // Object state

    private final AkibanInformationSchema ais;
    // Type of rowTypes is ArrayList, not List, to make it clear that null values are permitted.
    private final ArrayList<RowType> rowTypes = new ArrayList<RowType>();
    private volatile int typeIdCounter;
}
