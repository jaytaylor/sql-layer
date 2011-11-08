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
import com.akiban.util.ArgumentValidation;
import com.akiban.util.Tap;

import java.util.Collections;
import java.util.List;
import java.util.Set;

class Sort_Tree extends Operator
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("%s(%s)", getClass().getSimpleName(), sortType);
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
        return sortType;
    }

    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
        inputOperator.findDerivedTypes(derivedTypes);
        derivedTypes.add(sortType);
    }

    @Override
    public String describePlan()
    {
        return describePlan(inputOperator);
    }

    // Sort_Tree interface

    public Sort_Tree(Operator inputOperator, RowType sortType, API.Ordering ordering)
    {
        ArgumentValidation.notNull("sortType", sortType);
        ArgumentValidation.isGT("ordering.columns()", ordering.sortFields(), 0);
        this.inputOperator = inputOperator;
        this.sortType = sortType;
        this.ordering = ordering;
    }
    
    // Class state
    private static final Tap.PointTap SORT_TREE_COUNT = Tap.createCount("operator: sort_tree", true);

    // Object state

    private final Operator inputOperator;
    private final RowType sortType;
    private final API.Ordering ordering;

    // Inner classes

    private class Execution extends OperatorExecutionBase implements Cursor
    {
        // Cursor interface

        @Override
        public void open(Bindings bindings)
        {
            assert closed;
            input.open(bindings);
            this.bindings = bindings;
            closed = false;
        }

        @Override
        public Row next()
        {
            checkQueryCancelation();
            if (output == null) {
                SORT_TREE_COUNT.hit();
                output = adapter.sort(input, sortType, ordering, bindings);
            }
            Row row = null;
            if (!closed) {
                row = output.next();
                if (row == null) {
                    close();
                }
            }
            return row;
        }

        @Override
        public void close()
        {
            if (!closed) {
                input.close();
                if (output != null) {
                    output.close();
                    output = null;
                }
                closed = true;
            }
        }

        // Execution interface

        Execution(StoreAdapter adapter, Cursor input)
        {
            super(adapter);
            this.input = input;
        }

        // Object state

        private final Cursor input;
        private Cursor output;
        private Bindings bindings;
        private boolean closed = true;
    }
}
