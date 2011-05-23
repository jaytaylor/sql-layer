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

package com.akiban.qp.physicaloperator;

import com.akiban.qp.exec.Plannable;
import com.akiban.qp.rowtype.RowType;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public abstract class PhysicalOperator implements Plannable
{
    // I'm not sure I like having this as part of the interface. On one hand, operators like Flatten create new
    // RowTypes and it's handy to get access to those new RowTypes. On the other hand, not all operators do this,
    // and it's conceivable we'll have to invent an operator for which this doesn't make sense, e.g., it creates
    // multiple RowTypes.
    public RowType rowType()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<PhysicalOperator> getInputOperators()
    {
        return Collections.emptyList();
    }

    protected abstract Cursor cursor(StoreAdapter adapter);

    public boolean cursorAbilitiesInclude(CursorAbility ability) {
        return false;
    }

    @Override
    public String describePlan()
    {
        return toString();
    }

    @Override
    public final String describePlan(PhysicalOperator inputOperator)
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append(inputOperator.describePlan());
        buffer.append('\n'); // the newline separator isn't necessarily \u000a, but that's okay
        buffer.append(toString());
        return buffer.toString();
    }
}
