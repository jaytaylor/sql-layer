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

import com.akiban.sql.optimizer.explain.Explainer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import com.akiban.qp.row.BindableRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.sql.optimizer.explain.Attributes;
import com.akiban.sql.optimizer.explain.Label;
import com.akiban.sql.optimizer.explain.OperationExplainer;
import com.akiban.sql.optimizer.explain.PrimitiveExplainer;
import com.akiban.sql.optimizer.explain.Type;
import com.akiban.util.tap.InOutTap;
import java.util.Map;

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

    @Override
    public Explainer getExplainer(Map extraInfo)
    {
        Attributes att = new Attributes();
        
        att.put(Label.NAME, PrimitiveExplainer.getInstance("Values Scan"));
        for (BindableRow row : rows)
        {
            att.put(Label.ROWTYPE, PrimitiveExplainer.getInstance(row.toString()));
        }
        
        return new OperationExplainer(Type.SCAN_OPERATOR, att);
    }
    
    private static class Execution extends OperatorExecutionBase implements Cursor
    {
        private final Collection<? extends BindableRow> rows;
        private Iterator<? extends BindableRow> iter;
        private boolean destroyed = false;

        public Execution (QueryContext context, Collection<? extends BindableRow> rows) {
            super(context);
            this.rows = rows;
        }

        @Override
        public void close() {
            CursorLifecycle.checkIdleOrActive(this);
            iter = null;
        }

        @Override
        public Row next() {
            TAP_NEXT.in();
            try {
                CursorLifecycle.checkIdleOrActive(this);
                if (iter != null && iter.hasNext()) {
                    return iter.next().bind(context);
                } else {
                    close();
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
                CursorLifecycle.checkIdle(this);
                iter = rows.iterator();
            } finally {
                TAP_OPEN.out();
            }
        }

        @Override
        public void destroy()
        {
            close();
            destroyed = true;
        }

        @Override
        public boolean isIdle()
        {
            return !destroyed && iter == null;
        }

        @Override
        public boolean isActive()
        {
            return !destroyed && iter != null;
        }

        @Override
        public boolean isDestroyed()
        {
            return destroyed;
        }
    }
}
