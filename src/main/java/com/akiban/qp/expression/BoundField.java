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

package com.akiban.qp.expression;

import com.akiban.qp.physicaloperator.Bindings;
import com.akiban.qp.row.Row;
import com.akiban.server.types.ValueSource;

class BoundField implements Expression
{
    // Expression interface

    @Override
    public Object evaluate(Row row, Bindings bindings)
    {
        Row rowFromBindings = (Row) bindings.get(rowPosition);
        ValueSource source = rowFromBindings.eval(fieldPosition);
        return ExpressionConversionHelper.objectFromValueSource(source);
    }

    @Override
    public String toString()
    {
        return String.format("BoundField(%s, %s)", rowPosition, fieldPosition);
    }

    // Field interface

    BoundField(int rowPosition, int fieldPosition)
    {
        this.rowPosition = rowPosition;
        this.fieldPosition = fieldPosition;
    }

    // Object state

    private final int rowPosition; // within bindings
    private final int fieldPosition; // within row retrieved from bindings
}
