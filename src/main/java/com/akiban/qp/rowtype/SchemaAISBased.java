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
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.expression.Expression;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.lang.Math.max;

// UserTable RowTypes are indexed by the UserTable's RowDef's ordinal. Derived RowTypes get higher values.

public class SchemaAISBased implements Schema
{
    // Schema interface

    @Override
    public synchronized UserTableRowType userTableRowType(UserTable table)
    {
        return (UserTableRowType) rowTypes.get(table.getTableId());
    }

    @Override
    public synchronized IndexRowType indexRowType(TableIndex index)
    {
        assert index.getTable().isUserTable() : index;
        return userTableRowType((UserTable) index.getTable()).indexRowType(index);
    }

    @Override
    public synchronized FlattenedRowType newFlattenType(RowType parent, RowType child)
    {
        return new FlattenedRowType(this, nextTypeId(), parent, child);
    }

    @Override
    public synchronized ProjectedRowType newProjectType(List<Expression> columns)
    {
        return new ProjectedRowType(this, nextTypeId(), columns);
    }

    @Override
    public ProductRowType newProductType(RowType left, RowType right)
    {
        return new ProductRowType(this, nextTypeId(), left, right);
    }

    @Override
    public synchronized Iterator<RowType> rowTypes()
    {
        return rowTypes.values().iterator();
    }

    // SchemaAISBased interface

    public SchemaAISBased(AkibanInformationSchema ais)
    {
        this.ais = ais;
        this.typeIdCounter = -1;
        // Create RowTypes for AIS UserTables
        for (UserTable userTable : ais.getUserTables().values()) {
            UserTableRowType userTableRowType = new UserTableRowType(this, userTable);
            int tableTypeId = userTableRowType.typeId();
            rowTypes.put(tableTypeId, userTableRowType);
            typeIdCounter = max(typeIdCounter, userTableRowType.typeId());
        }
        // Create RowTypes for AIS Indexes
        for (UserTable userTable : ais.getUserTables().values()) {
            UserTableRowType userTableRowType = userTableRowType(userTable);
            for (TableIndex index : userTable.getIndexesIncludingInternal()) {
                IndexRowType indexRowType = new IndexRowType(this, userTableRowType, index);
                userTableRowType.addIndexRowType(indexRowType);
                rowTypes.put(indexRowType.typeId(), indexRowType);
            }
        }
    }

    public AkibanInformationSchema ais()
    {
        return ais;
    }

    // For use by this package

    synchronized int nextTypeId()
    {
        return ++typeIdCounter;
    }

    // Object state

    private final AkibanInformationSchema ais;
    private final Map<Integer, RowType> rowTypes = new HashMap<Integer, RowType>();
    private volatile int typeIdCounter = 0;
}
