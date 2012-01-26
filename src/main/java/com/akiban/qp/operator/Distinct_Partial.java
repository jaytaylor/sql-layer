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
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.ShareHolder;

import java.util.*;

class Distinct_Partial extends Operator
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("%s(%s)", getClass().getSimpleName(), distinctType);
    }

    // Operator interface

    @Override
    public List<Operator> getInputOperators()
    {
        return Collections.singletonList(inputOperator);
    }

    @Override
    protected Cursor cursor(QueryContext context)
    {
        return new Execution(context, inputOperator.cursor(context));
    }

    @Override
    public RowType rowType()
    {
        return distinctType;
    }

    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
        inputOperator.findDerivedTypes(derivedTypes);
        derivedTypes.add(distinctType);
    }

    @Override
    public String describePlan()
    {
        return describePlan(inputOperator);
    }

    // Distinct_Partial interface

    public Distinct_Partial(Operator inputOperator, RowType distinctType)
    {
        ArgumentValidation.notNull("distinctType", distinctType);
        this.inputOperator = inputOperator;
        this.distinctType = distinctType;
    }

    // Object state

    private final Operator inputOperator;
    private final RowType distinctType;

    // Inner classes

    private class Execution extends OperatorExecutionBase implements Cursor
    {
        // Cursor interface

        @Override
        public void open()
        {
            input.open();
            nvalid = 0;
        }

        @Override
        public Row next()
        {
            checkQueryCancelation();
            Row row;
            while ((row = input.next()) != null) {
                assert row.rowType() == distinctType : row;
                if (isDistinct(row)) 
                    break;
            }
            return row;
        }

        @Override
        public void close()
        {
            input.close();
            currentRow.release();
        }

        // Execution interface

        Execution(QueryContext context, Cursor input)
        {
            super(context);
            this.input = input;

            nfields = distinctType.nFields();
            currentValues = new ValueHolder[nfields];
        }

        private boolean isDistinct(Row inputRow) 
        {
            if ((nvalid == 0) && currentRow.isEmpty()) {
                // Very first row.
                currentRow.hold(inputRow);
                return true;
            }
            ValueHolder inputValue = new ValueHolder();
            for (int i = 0; i < nfields; i++) {
                if (i == nvalid) {
                    assert currentRow.isHolding();
                    if (currentValues[i] == null)
                        currentValues[i] = new ValueHolder();
                    currentValues[i].copyFrom(currentRow.get().eval(i));
                    nvalid++;
                    if (nvalid == nfields)
                        // Once we have copies of all fields, don't need row any more.
                        currentRow.release();
                }
                inputValue.copyFrom(inputRow.eval(i));
                if (!currentValues[i].equals(inputValue)) {
                    currentValues[i] = inputValue;
                    nvalid = i + 1;
                    if (i < nfields - 1)
                        // Might need later fields.
                        currentRow.hold(inputRow);
                    return true;
                }
            }
            return false;
        }

        // Object state

        private final Cursor input;
        private final ShareHolder<Row> currentRow = new ShareHolder<Row>();
        private final int nfields;
        // currentValues contains copies of the first nvalid of currentRow's fields,
        // filled as needed.
        private int nvalid;
        private final ValueHolder[] currentValues;
    }
}
