package com.akiban.qp.operator;

import java.util.Collections;
import java.util.List;

import com.akiban.qp.row.Row;
import com.akiban.server.explain.Attributes;
import com.akiban.server.explain.CompoundExplainer;
import com.akiban.server.explain.ExplainContext;
import com.akiban.server.explain.std.DUIOperatorExplainer;
import com.akiban.util.tap.InOutTap;

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
for the HKEY to allow the persistit layer to lookup the row in the
btree to remove it. Failure results in a RowNotFoundException being
thrown and the operation aborted.

The operator assumes (but does not require) that all rows provided are
of the same RowType.

The Delete_Returning operator assumes (and requires) the input row types
be of a UserTableRowType, and not any derived type. This can't be
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
    protected Cursor cursor(QueryContext context) {
        return new Execution(context, inputOperator.cursor(context));
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

    public Delete_Returning (Operator inputOperator, boolean usePVals) {
        this.inputOperator = inputOperator;
        this.usePValues = usePVals;
    }
    // Class state
    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: DeleteReturning open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: DeleteReturning next");
    
    // Object state

    protected final Operator inputOperator;
    private final boolean usePValues;

    // Inner classes
    private class Execution extends OperatorExecutionBase implements Cursor
    {

        // Cursor interface
    
        @Override
        public void open()
        {
            TAP_OPEN.in();
            try {
                CursorLifecycle.checkIdle(this);
                input.open();
                idle = false;
            } finally {
                TAP_OPEN.out();
            }
        }
    
        @Override
        public Row next()
        {
            TAP_NEXT.in();
            try {
                CursorLifecycle.checkIdleOrActive(this);
                checkQueryCancelation();
                
                Row inputRow;
                if ((inputRow = input.next()) != null) {
                    adapter().deleteRow(inputRow, usePValues);
                }
                return inputRow; 
            } finally {
                TAP_NEXT.out();
            }
        }
    
        @Override
        public void close()
        {
            CursorLifecycle.checkIdleOrActive(this);
            if (!idle) {
                input.close();
                idle = true;
            }
        }
    
        @Override
        public void destroy()
        {
            if (input != null) {
                close();
                input.destroy();
                input = null;
            }
        }
    
        @Override
        public boolean isIdle()
        {
            return input != null && idle;
        }
    
        @Override
        public boolean isActive()
        {
            return input != null && !idle;
        }
    
        @Override
        public boolean isDestroyed()
        {
            return input == null;
        }
    
        // Execution interface
    
        Execution(QueryContext context, Cursor input)
        {
            super(context);
            this.input = input;
        }
    
        // Object state
    
        private Cursor input; // input = null indicates destroyed.
        private boolean idle = true;
    }
    
}
