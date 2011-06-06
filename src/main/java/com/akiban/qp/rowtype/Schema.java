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
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.expression.Expression;

import java.util.*;

import static java.lang.Math.max;

// UserTable RowTypes are indexed by the UserTable's RowDef's ordinal. Derived RowTypes get higher values.

public class Schema
{
    public Schema(AkibanInformationSchema ais)
    {
        this.ais = ais;
        this.typeIdCounter = -1;
        for (UserTable userTable : ais.getUserTables().values()) {
            UserTableRowType userTableRowType = new UserTableRowType(this, userTable);
            rowTypes.put(userTable.getTableId(), userTableRowType);
            this.typeIdCounter = max(this.typeIdCounter, userTable.getTableId());
            // Indexes
            this.typeIdCounter++;
            for (TableIndex index : userTable.getIndexesIncludingInternal()) {
                IndexRowType indexRowType = new IndexRowType(this, userTableRowType, index);
                userTableRowType.addIndexRowType(indexRowType);
                rowTypes.put(indexRowType.typeId(), indexRowType);
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
        return (UserTableRowType) rowTypes.get(table.getTableId());
    }

    public synchronized IndexRowType indexRowType(TableIndex index)
    {
        assert index.getTable().isUserTable() : index;
        return userTableRowType((UserTable) index.getTable()).indexRowType(index);
    }

    public synchronized FlattenedRowType newFlattenType(RowType parent, RowType child)
    {
        return new FlattenedRowType(this, typeIdCounter++, parent, child);
    }

    public synchronized ProjectedRowType newProjectType(List<Expression> columns)
    {
        return new ProjectedRowType(this, typeIdCounter++, columns);
    }

    public synchronized Collection<RowType> rowTypes()
    {
        return rowTypes.values();
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

    // Object state

    private final AkibanInformationSchema ais;
    // Type of rowTypes is ArrayList, not List, to make it clear that null values are permitted.
    private final Map<Integer, RowType> rowTypes = new HashMap<Integer, RowType>();
    private volatile int typeIdCounter;
}
