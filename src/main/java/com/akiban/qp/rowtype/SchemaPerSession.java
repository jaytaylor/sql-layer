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

import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.expression.Expression;
import com.akiban.util.MultiIterator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SchemaPerSession implements Schema
{
    // Schema interface

    @Override
    public synchronized UserTableRowType userTableRowType(UserTable table)
    {
        UserTableRowType rowType = (UserTableRowType) rowTypes.get(table.getTableId());
        if (rowType == null) {
            rowType = aisSchema.userTableRowType(table);
            rowTypes.put(table.getTableId(), rowType);
        }
        return rowType;
    }

    @Override
    public synchronized IndexRowType indexRowType(TableIndex index)
    {
        return aisSchema.indexRowType(index);
    }

    @Override
    public synchronized FlattenedRowType newFlattenType(RowType parent, RowType child)
    {
        FlattenedRowType rowType = aisSchema.newFlattenType(parent, child);
        rowTypes.put(rowType.typeId(), rowType);
        return rowType;
    }

    @Override
    public synchronized ProjectedRowType newProjectType(List<Expression> columns)
    {
        ProjectedRowType rowType = aisSchema.newProjectType(columns);
        rowTypes.put(rowType.typeId(), rowType);
        return rowType;
    }

    @Override
    public ProductRowType newProductType(RowType left, RowType right)
    {
        ProductRowType rowType = aisSchema.newProductType(left, right);
        rowTypes.put(rowType.typeId(), rowType);
        return rowType;
    }

    @Override
    public synchronized Iterator<RowType> rowTypes()
    {
        return new MultiIterator<RowType>(aisSchema.rowTypes(), rowTypes.values().iterator());
    }

    // SchemaAISBased interface

    public SchemaPerSession(SchemaAISBased aisSchema)
    {
        this.aisSchema = aisSchema;
    }

    // Object state

    private final SchemaAISBased aisSchema;
    private final Map<Integer, RowType> rowTypes = new HashMap<Integer, RowType>();
}
