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
import com.akiban.ais.model.UserTable;
import com.akiban.server.RowDef;

import java.util.ArrayList;

import static java.lang.Math.max;

// UserTable RowTypes are indexed by the UserTable's RowDef's ordinal. Derived RowTypes get higher values.

public class Schema
{
    public Schema(AkibanInformationSchema ais)
    {
        typeIdCounter = -1;
        for (UserTable userTable : ais.getUserTables().values()) {
            RowDef rowDef = (RowDef) userTable.rowDef();
            int ordinal = rowDef.getOrdinal();
            int requiredEntries = ordinal + 1;
            while (rowTypes.size() < requiredEntries) {
                rowTypes.add(null);
            }
            rowTypes.set(ordinal, new UserTableRowType(this, userTable));
            typeIdCounter = max(typeIdCounter, ordinal);
        }
        typeIdCounter++;
    }

    public synchronized UserTableRowType userTableRowType(UserTable table)
    {
        RowDef rowDef = (RowDef) table.rowDef();
        return (UserTableRowType) rowTypes.get(rowDef.getOrdinal());
    }

    public synchronized FlattenedRowType newFlattenType(RowType parent, RowType child)
    {
        return new FlattenedRowType(this, typeIdCounter++, parent, child);
    }

    // Object state

    // Field type is ArrayList, not List, to make it clear that null values are permitted.
    private final ArrayList<RowType> rowTypes = new ArrayList<RowType>();
    private volatile int typeIdCounter;
}
