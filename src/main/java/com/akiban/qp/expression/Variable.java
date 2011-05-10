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
import com.akiban.qp.row.RowBase;

final class Variable implements Expression {

    // Expression interface

    @Override
    public Object evaluate(RowBase row, Bindings bindings) {
        return bindings.get(position);
    }

    // Object interface

    @Override
    public String toString() {
        return String.format("Variable(pos=%d)", position);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Variable variable = (Variable) o;

        return position == variable.position;

    }

    @Override
    public int hashCode() {
        return position;
    }

    // Variable interface

    public Variable(int position) {
        this.position = position;
    }

    // Object state

    private final int position;
}
