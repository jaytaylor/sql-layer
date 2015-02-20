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

The Delete_Returning deletes rows from a given table. Every row provided
by the input operator is sent to the <i>StoreAdapter#deleteRow()</i>
method to be removed from the table.

<h1>Arguments</h1>

<ul>

<li><b>input:</b> the input operator supplying rows to be deleted.

</ul>

<h1>Behaviour</h1>

Rows supplied by the input operator are deleted from the underlying
data store through the StoreAdapter interface.

<h1>Output</h1>

The rows deleted are returned through the cursor interface. 

<h1>Assumptions</h1>

The rows provided by the input operator includes all of the columns
for the HKEY to allow the full row to be looked up. Failure results
in a RowNotFoundException being thrown and the operation aborted.

The operator assumes (but does not require) that all rows provided are
of the same RowType.

The Delete_Returning operator assumes (and requires) the input row types
be of a TableRowType, and not any derived type. This can't be
enforced by the constructor because <i>PhysicalOperator#rowType()</i>
isn't implemented for all operators.

<h1>Performance</h1>

Deletion assumes the data store needs to alter the underlying storage
system, including any system change log. This requires multiple IOs
per operation.

<h1>Memory Requirements</h1>

Each row is individually processed.

*/

public class Delete_Returning extends Operator {


    @Override
    protected Cursor cursor(QueryContext context, QueryBindingsCursor bindingsCursor) {
        return new Execution(context, inputOperator.cursor(context, bindingsCursor));
    }

    @Override
    public List<Operator> getInputOperators() {
        return Collections.singletonList(inputOperator);
    }

    @Override
    public String toString() {
        return String.format("%s(%s)", getClass().getSimpleName(), inputOperator);
    }


    @Override
    public CompoundExplainer getExplainer(ExplainContext context) {
        Attributes atts = new Attributes();
        if (context.hasExtraInfo(this))
            atts.putAll(context.getExtraInfo(this).get()); 
        return new DUIOperatorExplainer(getName(), atts, inputOperator, context);
    }

    public Delete_Returning (Operator inputOperator, boolean cascadeDelete) {
        this.inputOperator = inputOperator;
        this.cascadeDelete = cascadeDelete; 
    }

    // Class state
    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: DeleteReturning open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: DeleteReturning next");
    private static final Logger LOG = LoggerFactory.getLogger(Delete_Returning.class);

    // Object state

    protected final Operator inputOperator;
    private final boolean cascadeDelete;

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
                    adapter().deleteRow(inputRow, cascadeDelete);
                    if (LOG_EXECUTION) {
                        LOG.debug("Delete_Returning: deleting {}", inputRow);
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
