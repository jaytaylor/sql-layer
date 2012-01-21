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

package com.akiban.qp.operator;

import com.akiban.qp.row.Row;
import com.akiban.qp.row.ValuesRow;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.ValuesRowType;
import com.akiban.server.types.AkType;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.tap.Tap;

import java.util.Collections;
import java.util.List;
import java.util.Set;

class Count_Default extends Operator
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("%s(%s)", getClass().getSimpleName(), countType);
    }

    // Operator interface

    @Override
    public List<Operator> getInputOperators()
    {
        return Collections.singletonList(inputOperator);
    }

    @Override
    protected Cursor cursor(StoreAdapter adapter)
    {
        return new Execution(adapter, inputOperator.cursor(adapter));
    }

    @Override
    public RowType rowType()
    {
        return resultType;
    }

    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
        inputOperator.findDerivedTypes(derivedTypes);
        derivedTypes.add(resultType);
    }

    @Override
    public String describePlan()
    {
        return describePlan(inputOperator);
    }

    // Count_Default interface

    public Count_Default(Operator inputOperator, RowType countType)
    {
        ArgumentValidation.notNull("countType", countType);
        this.inputOperator = inputOperator;
        this.countType = countType;
        this.resultType = countType.schema().newValuesType(AkType.LONG);
    }
    
    // Class state
    
    private static final Tap.PointTap COUNT_COUNT = Tap.createCount("operator: count", true);

    // Object state

    private final Operator inputOperator;
    private final RowType countType;
    private final ValuesRowType resultType;

    // Inner classes

    private class Execution extends OperatorExecutionBase implements Cursor
    {
        // Cursor interface

        @Override
        public void open(Bindings bindings)
        {
            COUNT_COUNT.hit();
            input.open(bindings);
            count = 0;
            closed = false;
        }

        @Override
        public Row next()
        {
            checkQueryCancelation();
            Row row = null;
            while ((row == null) && !closed) {
                row = input.next();
                if (row == null) {
                    close();
                    row = new ValuesRow(resultType, new Object[] { count });
                } else if (row.rowType() == countType) {
                    row = null;
                    count++;
                }
            }
            return row;
        }

        @Override
        public void close()
        {
            input.close();
            closed = true;
        }

        // Execution interface

        Execution(StoreAdapter adapter, Cursor input)
        {
            super(adapter);
            this.input = input;
        }

        // Object state

        private final Cursor input;
        private long count;
        private boolean closed;
    }
}
