/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */
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

Inserts new rows into a table. 

<h1>Arguments</h1>

<ul>

<li><b>PhysicalOperator inputOperator:</b> source of rows to be inserted

</ul>

<h1>Behaviour</h1>

For each row in the insert operator, the row in inserted into the
table. In practice, this is currently done via
<i>StoreAdapater#insertRow</i>, which is implemented by
<i>PersistitAdapater#insertRow</i>, which invokes
<i>PersistitStore#insertRow</i>

As next is called, each row is inserted as a side effect of pulling
rows through the InsertReturning operator. Rows are returned unchanged. 

<h1>Output</h1>

Rows that have been inserted into the StoreAdapter.

<h1>Assumptions</h1>

The inputOperator is returning rows of the UserTableRowType of the table being inserted into.

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
    protected Cursor cursor(QueryContext context) {
        return new Execution(context, inputOperator.cursor(context));
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
    
    public Insert_Returning (Operator inputOperator, boolean usePValues) {
        this.inputOperator = inputOperator;
        this.usePValues = usePValues;
    }
    
    
    // Class state
    
    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: InsertReturning open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: InsertReturning next");
    
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
                    // TODO: Perform constraint check for insert here
                    // Needs to be moved to Constraint Check operator. 
                    context.checkConstraints(inputRow, usePValues);
                    // Do the real work of inserting the row
                    adapter().writeRow(inputRow, usePValues);
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
