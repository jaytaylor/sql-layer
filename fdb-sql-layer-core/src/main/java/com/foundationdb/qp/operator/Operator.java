/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.qp.operator;

import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.exec.Plannable;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.util.tap.InOutTap;
import com.foundationdb.util.tap.Tap;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public abstract class Operator implements Plannable
{
    // Object interface

    @Override
    public String toString()
    {
        return getName();
    }

    // Operator interface

    @Override
    public String getName()
    {
        return getClass().getSimpleName();
    }

    // I'm not sure I like having this as part of the interface. On one hand, operators like Flatten create new
    // RowTypes and it's handy to get access to those new RowTypes. On the other hand, not all operators do this,
    // and it's conceivable we'll have to invent an operator for which this doesn't make sense, e.g., it creates
    // multiple RowTypes.
    public RowType rowType()
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Find the derived types created by this operator and its inputs. A <i>derived type</i> is a type generated
     * by an operator, and as such, does not correspond to an AIS Table or Index.
     * @param derivedTypes Derived types created by this operator or input operators are added to derivedTypes.
     */
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
    }

    @Override
    public List<Operator> getInputOperators()
    {
        return Collections.emptyList();
    }

    protected abstract Cursor cursor(QueryContext context, QueryBindingsCursor bindingsCursor);

    @Override
    public String describePlan()
    {
        return toString();
    }

    @Override
    public final String describePlan(Operator inputOperator)
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append(inputOperator.describePlan());
        buffer.append(NL);
        buffer.append(toString());
        return buffer.toString();
    }

    // For use by subclasses

    protected int ordinal(Table table)
    {
        return table.getOrdinal();
    }

    // Class state

    protected static final String NL = System.getProperty("line.separator");
    public static final InOutTap OPERATOR_TAP = Tap.createRecursiveTimer("operator: root");
}
