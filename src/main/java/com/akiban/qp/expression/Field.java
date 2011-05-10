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

import com.akiban.qp.physicaloperator.UndefBindings;
import com.akiban.qp.row.RowBase;

class Field implements Expression
{
    // Expression interface

    @Override
    public Object evaluate(RowBase row)
    {
        return row.field(position, UndefBindings.only()); // TODO evaluate needs to take a Bindings, too
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "(" + position + ")";
    }

    // Field interface

    Field(int position)
    {
        this.position = position;
    }

    // Object state

    private final int position;
}
