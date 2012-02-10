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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import com.akiban.qp.row.BindableRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.tap.InOutTap;

/**

 <h1>Overview</h1>

 ValuesScan_Default is an in-memory collection of identical rows used
 as a source operator.

 <h1>Arguments</h1>

 <ul>

 <li><b>List<ExpressionRow> rows:</b> the list of ExpressionRows to be
 returned by the cursor in order

 <h1>Behaviour </h1>

 The rows are returned in the order they are present in the list.

 <h1>Output</h1>

 Rows as given

 <h1>Assumptions</h1>

 None

 <h1>Performance</h1>

 No I/O cost, as the list is maintained in memory.

 <h1>Memory Requirements</h1>

 Memory requirement is for the number of rows stored in the list
 supplied. There are no memory requirement beyond that.

 */

public class ValuesScan_Default extends Operator
{

    // Operator interface

    @Override
    public RowType rowType() {
        return rowType;
    }

    @Override
    protected Cursor cursor(QueryContext context) {
        return new Execution(context, rows);
    }
    
    @Override
    public String toString()
    {
        return getClass().getSimpleName()  + rows;
    }

    public ValuesScan_Default (Collection<? extends BindableRow> bindableRows, RowType rowType) {
        this.rows = new ArrayList<BindableRow>(bindableRows);
        this.rowType = rowType;
    }

    // Class state
    
    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: ValuesScan_Default open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: ValuesScan_Default next");
    
    // Object state
    
    private final Collection<? extends BindableRow> rows;
    private final RowType rowType;
    
    private static class Execution extends OperatorExecutionBase implements Cursor
    {
        private final Collection<? extends BindableRow> rows;
        private Iterator<? extends BindableRow> iter;

        public Execution (QueryContext context, Collection<? extends BindableRow> rows) {
            super(context);
            this.rows = rows;
        }

        @Override
        public void close() {
            iter = null;
        }

        @Override
        public Row next() {
            TAP_NEXT.in();
            try {
                if (iter != null && iter.hasNext()) {
                    return iter.next().bind(context);
                } else {
                    return null;
                }
            } finally {
                TAP_NEXT.out();
            }
        }

        @Override
        public void open() {
            TAP_OPEN.in();
            try {
                iter = rows.iterator();
            } finally {
                TAP_OPEN.out();
            }
        }
    }
}
