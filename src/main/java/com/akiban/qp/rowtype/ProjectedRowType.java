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

import com.akiban.qp.expression.Expression;

import java.util.List;

public class ProjectedRowType extends DerivedRowType
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("project(%s)", projections);
    }


    // RowType interface

    @Override
    public int nFields()
    {
        return projections.size();
    }

    // ProjectedRowType interface

    public ProjectedRowType(Schema schema, int typeId, List<Expression> projections)
    {
        super(schema, typeId);
        this.projections = projections;
    }

    // Object state

    private final List<Expression> projections;
}
