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

import com.akiban.qp.row.Row;

public class Field implements Expression
{
    // Expression interface

    @Override
    public Object evaluate(Row row)
    {
        return row.field(position);
    }

    // Field interface

    public Field(int position)
    {
        this.position = position;
    }

    // Object state

    private final int position;
}
