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

import java.util.Collections;
import java.util.List;

import com.foundationdb.qp.row.Row;
import com.foundationdb.server.explain.Attributes;
import com.foundationdb.server.explain.CompoundExplainer;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.std.DUIOperatorExplainer;
import com.foundationdb.util.tap.InOutTap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**

<h1>Overview</h1>

Inserts new rows into a table. 

<h1>Arguments</h1>

<ul>

<li><b>PhysicalOperator inputOperator:</b> source of rows to be inserted

</ul>

<h1>Behaviour</h1>

For each row in the insert operator, the row in inserted into the
table. In practice, this is currently done via {@link StoreAdapter#writeRow}.

As next is called, each row is inserted as a side effect of pulling
rows through the InsertReturning operator. Rows are returned unchanged. 

<h1>Output</h1>

Rows that have been inserted into the StoreAdapter.

<h1>Assumptions</h1>

The inputOperator is returning rows of the TableRowType of the table being inserted into.

The inputOperator has already placed all the values for the row that need to be written. 

<h1>Performance</h1>

Insertion assumes the data store needs to alter the underlying storage
system, including any system change log. This requires multiple IOs
per operation.

Insert may be slow because because indexes are also updated. Insert
may be able to be improved in performance by batching the index
updates, but there is no current API to so.

<h1>Memory Requirements</h1>

None.

*/

public class Insert_Returning extends Operator {

    @Override
    protected Cursor cursor(QueryContext context, QueryBindingsCursor bindingsCursor) {
        return new Execution(context, inputOperator.cursor(context, bindingsCursor));
    }

    @Override
    public List<Operator> getInputOperators()
    {
        return Collections.singletonList(inputOperator);
    }

    @Override
    public String toString()
    {
        return String.format("%s(%s)", getClass().getSimpleName(), inputOperator);
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        Attributes atts = new Attributes();
        if (context.hasExtraInfo(this))
            atts.putAll(context.getExtraInfo(this).get()); 
        return new DUIOperatorExplainer(getName(), atts, inputOperator, context);
    }

    public Insert_Returning (Operator inputOperator) {
        this.inputOperator = inputOperator;
    }
    
    
    // Class state
    
    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: InsertReturning open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: InsertReturning next");
    private static final Logger LOG = LoggerFactory.getLogger(Insert_Returning.class);

    // Object state

    protected final Operator inputOperator;

    // Inner classes
    private class Execution extends ChainedCursor
    {

        // Cursor interface
    
        @Override
        public void open()
        {
            TAP_OPEN.in();
            try {
                super.open();
            } finally {
                TAP_OPEN.out();
            }
        }
    
        @Override
        public Row next()
        {
            if (TAP_NEXT_ENABLED) {
                TAP_NEXT.in();
            }
            try {
                if (CURSOR_LIFECYCLE_ENABLED) {
                    CursorLifecycle.checkIdleOrActive(this);
                }
                checkQueryCancelation();
                
                Row inputRow;
                if ((inputRow = input.next()) != null) {
                    // Do the real work of inserting the row
                    adapter().writeRow(inputRow);
                    if (LOG_EXECUTION) {
                        LOG.debug("Insert_Returning: inserting {}", inputRow);
                    }
                }
                return inputRow; 
            } finally {
                if (TAP_NEXT_ENABLED) {
                    TAP_NEXT.out();
                }
            }
        }
    
        // Execution interface
    
        Execution(QueryContext context, Cursor input)
        {
            super(context, input);
        }
    
        // Object state
    }
}
