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
import com.akiban.util.tap.InOutTap;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 <h1>Overview</h1>

 Sort_Tree generates an output stream containing all the rows of the input stream, sorted according to an
 ordering specification. The "Tree" in the name refers to the implementation, in which the rows are inserted
 into a B-tree (presumably on-disk) and then read out in order.

 <h1>Arguments</h1>

 <li><b>Operator inputOperator:</b> Operator providing the input stream.
 <li><b>RowType sortType:</b> Type of rows to be sorted.
 <li><b>API.Ordering ordering:</b> Specification of ordering, comprising a list of expressions and ascending/descending
 specifications.
 <li><b>API.SortOption sortOption:</b> Specifies whether duplicates should be kept (PRESERVE_DUPLICATES) or eliminated
 (SUPPRESS_DUPLICATES)

 <h1>Behavior</h1>

 The rows of the input stream are written into a B-tree that orders rows according to the ordering specification.
 Once the input stream has been consumed, the B-tree is traversed from beginning to end to provide rows of the output
 stream.

 <h1>Output</h1>

 The rows of the input stream, sorted according to the ordering specification. Duplicates are eliminated if
 and only if the sortOption is SUPPRESS_DUPLICATES.

 <h1>Assumptions</h1>

 None.

 <h1>Performance</h1>

 Sort_Tree generates IO dependent on the size of the input stream. This occurs mostly during the loading phase,
 (when the input stream is being read). There will be some IO when the loaded B-tree is scanned, but this is
 expected to be more efficient, as each page will be read completely before moving on to the next one.

 <h1>Memory Requirements</h1>

 Memory requirements (and disk requirements) depend on the underlying B-tree.

 */

class Sort_Tree extends Operator
{
    // Object interface

    @Override
    public String toString()
    {
        if (sortOption == API.SortOption.PRESERVE_DUPLICATES)
            return String.format("%s(%s)", getClass().getSimpleName(), sortType);
        else
            return String.format("%s(%s, %s)", getClass().getSimpleName(), sortType, sortOption.name());
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

    public Sort_Tree(Operator inputOperator, RowType sortType, API.Ordering ordering, API.SortOption sortOption)
    {
        ArgumentValidation.notNull("sortType", sortType);
        ArgumentValidation.isGT("ordering.columns()", ordering.sortColumns(), 0);
        this.inputOperator = inputOperator;
        this.sortType = sortType;
        this.ordering = ordering;
        this.sortOption = sortOption;
    }
    
    // Class state

    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: Sort_Tree open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: Sort_Tree next");
    private static final InOutTap TAP_LOAD = OPERATOR_TAP.createSubsidiaryTap("operator: Sort_Tree load");

    // Object state

    private final Operator inputOperator;
    private final RowType sortType;
    private final API.Ordering ordering;
    private final API.SortOption sortOption;

    // Inner classes

    private class Execution extends OperatorExecutionBase implements Cursor
    {
        // Cursor interface

        @Override
        public void open()
        {
            assert closed;
            TAP_OPEN.in();
            try {
                input.open();
                closed = false;
            } catch (Exception e) {
                TAP_OPEN.out();
            }
        }

        @Override
        public Row next()
        {
            checkQueryCancelation();
            if (output == null) {
                output = adapter().sort(context, input, sortType, ordering, sortOption, TAP_LOAD);
            }
            Row row = null;
            if (!closed) {
                TAP_NEXT.in();
                try {
                    row = output.next();
                } finally {
                    TAP_NEXT.out();
                }
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

        Execution(QueryContext context, Cursor input)
        {
            super(context);
            this.input = input;
        }

        // Object state

        private final Cursor input;
        private Cursor output;
        private boolean closed = true;
    }
}
