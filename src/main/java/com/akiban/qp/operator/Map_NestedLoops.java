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

import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.sql.optimizer.explain.Explainer;
import com.akiban.sql.optimizer.explain.Label;
import com.akiban.sql.optimizer.explain.PrimitiveExplainer;
import com.akiban.sql.optimizer.explain.std.NestedLoopsExplainer;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.ShareHolder;
import com.akiban.util.tap.InOutTap;
import java.math.BigDecimal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**

 <h1>Overview</h1>

 Map_NestedLoops implements a mapping using a nested-loop algorithm. The left input operator (outer loop)
 provides a stream of input rows. The right input operator (inner loop) binds this row, and 
 its output, combined across all input rows, forms the Map_NestedLoops output stream. 
 
 <h1>Arguments</h1>

 <ul>

 <li><b>Operator outerInputOperator:</b> Provides stream of input.

 <li><b>Operator innerInputOperator:</b> Provides Map_NestedLoops output.

 <li><b>int inputBindingPosition:</b> Position of inner loop row in query context.

 </ul>

 <h1>Behavior</h1>

 The outer input operator provides a stream of rows. Each is bound in turn to the query context, in the 
 position specified by inputBindingPosition. For each input row, the inner operator binds the row and executes,
 yielding a stream of output rows. The concatenation of these streams comprises the output from Map_NestedLoops.
 
 The inner operator is used multiple times, once for each input row. On each iteration, the input row is bound
 to the query context, the inner cursor is opened, and then the inner cursor is consumed.

 <h1>Output</h1>

 The concatenation of streams from the inner operator.

 <h1>Assumptions</h1>

 None.

 <h1>Performance</h1>

 Product_NestedLoops does no IO.

 <h1>Memory Requirements</h1>

 A single row from the outer loops is stored at all times.
 
 */


class Map_NestedLoops extends Operator
{
    // Operator interface

    @Override
    protected Cursor cursor(QueryContext context)
    {
        return new Execution(context);
    }

    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
        innerInputOperator.findDerivedTypes(derivedTypes);
        outerInputOperator.findDerivedTypes(derivedTypes);
    }

    @Override
    public List<Operator> getInputOperators()
    {
        List<Operator> result = new ArrayList<Operator>(2);
        result.add(outerInputOperator);
        result.add(innerInputOperator);
        return result;
    }

    @Override
    public String describePlan()
    {
        return String.format("%s\n%s", describePlan(outerInputOperator), describePlan(innerInputOperator));
    }

    // Project_Default interface

    public Map_NestedLoops(Operator outerInputOperator,
                           Operator innerInputOperator,
                           int inputBindingPosition)
    {
        ArgumentValidation.notNull("outerInputOperator", outerInputOperator);
        ArgumentValidation.notNull("innerInputOperator", innerInputOperator);
        ArgumentValidation.isGTE("inputBindingPosition", inputBindingPosition, 0);
        this.outerInputOperator = outerInputOperator;
        this.innerInputOperator = innerInputOperator;
        this.inputBindingPosition = inputBindingPosition;
    }

    // Class state

    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: Map_NestedLoops open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: Map_NestedLoops next");
    private static final Logger LOG = LoggerFactory.getLogger(Map_NestedLoops.class);

    // Object state

    private final Operator outerInputOperator;
    private final Operator innerInputOperator;
    private final int inputBindingPosition;

    @Override
    public Explainer getExplainer(Map extraInfo)
    {
        Explainer ex = new NestedLoopsExplainer("Map_NestedLoops", innerInputOperator, outerInputOperator, null, null, extraInfo);
        ex.addAttribute(Label.BINDING_POSITION, PrimitiveExplainer.getInstance(inputBindingPosition));
        return ex;
    }

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
                this.outerInput.open();
                this.closed = false;
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
                Row outputRow = null;
                while (!closed && outputRow == null) {
                    outputRow = nextOutputRow();
                    if (outputRow == null) {
                        Row row = outerInput.next();
                        if (row == null) {
                            close();
                        } else {
                            outerRow.hold(row);
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Map_NestedLoops: restart inner loop using current branch row");
                            }
                            startNewInnerLoop(row);
                        }
                    }
                }
                if(LOG.isDebugEnabled()) {
                    LOG.debug("Map_NestedLoops: yield {}", outputRow);
                }
                return outputRow;
            } finally {
                TAP_NEXT.out();
            }
        }

        @Override
        public void close()
        {
            CursorLifecycle.checkIdleOrActive(this);
            if (!closed) {
                innerInput.close();
                closeOuter();
                closed = true;
            }
        }

        @Override
        public void destroy()
        {
            close();
            innerInput.destroy();
            outerInput.destroy();
        }

        @Override
        public boolean isIdle()
        {
            return outerInput.isIdle();
        }

        @Override
        public boolean isActive()
        {
            return outerInput.isActive();
        }

        @Override
        public boolean isDestroyed()
        {
            return outerInput.isDestroyed();
        }

        // Execution interface

        Execution(QueryContext context)
        {
            super(context);
            this.outerInput = outerInputOperator.cursor(context);
            this.innerInput = innerInputOperator.cursor(context);
        }

        // For use by this class

        private Row nextOutputRow()
        {
            Row outputRow = null;
            if (outerRow.isHolding()) {
                Row innerRow = innerInput.next();
                if (innerRow == null) {
                    outerRow.release();
                } else {
                    outputRow = innerRow;
                }
            }
            return outputRow;
        }

        private void closeOuter()
        {
            outerRow.release();
            outerInput.close();
        }

        private void startNewInnerLoop(Row row)
        {
            innerInput.close();
            context.setRow(inputBindingPosition, row);
            innerInput.open();
        }

        // Object state

        private final Cursor outerInput;
        private final Cursor innerInput;
        private final ShareHolder<Row> outerRow = new ShareHolder<Row>();
        private boolean closed = true;
    }
}
