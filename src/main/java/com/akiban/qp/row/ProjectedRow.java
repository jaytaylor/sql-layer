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

package com.akiban.qp.row;

import com.akiban.qp.expression.Expression;
import com.akiban.qp.physicaloperator.Bindings;
import com.akiban.qp.rowtype.FlattenedRowType;
import com.akiban.qp.rowtype.ProjectedRowType;
import com.akiban.qp.rowtype.RowType;

import java.util.List;

public class ProjectedRow extends AbstractRow
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("%s[%s]", row, projections);
    }

    // Row interface

    @Override
    public RowType rowType()
    {
        return rowType;
    }

    @Override
    public Object field(int i, Bindings bindings)
    {
        return projections.get(i).evaluate(row.get(), bindings);
    }

    @Override
    public HKey hKey()
    {
        throw new UnsupportedOperationException();
    }

    // ProjectedRow interface

    public ProjectedRow(ProjectedRowType rowType, Row row, List<Expression> projections)
    {
        this.rowType = rowType;
        this.row.set(row);
        this.projections = projections;
    }

    // Object state

    private final ProjectedRowType rowType;
    private final RowHolder<Row> row = new RowHolder<Row>();
    private final List<Expression> projections;
}
