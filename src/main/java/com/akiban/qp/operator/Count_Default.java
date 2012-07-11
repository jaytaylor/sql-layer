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
import com.akiban.qp.row.ValuesRow;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.ValuesRowType;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.sql.optimizer.explain.Explainer;
import com.akiban.sql.optimizer.explain.std.CountOperatorExplainer;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.tap.InOutTap;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**

 <h1>Overview</h1>

 Count_Default counts the number of rows of a specified RowType.

 <h1>Arguments</h1>

 <ul>

 <li><b>RowType countType:</b> Type of rows to be counted.

 </ul>


 <h1>Behavior</h1>

 The input rows whose type matches the countType are counted.

 <h1>Output</h1>

 A single row containing the row count (type long).

 <h1>Assumptions</h1>

 None.

 <h1>Performance</h1>

 This operator does no IO.

 <h1>Memory Requirements</h1>

 This operator keeps no rows in memory.

 */

class Count_Default extends Operator
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("%s(%s)", getClass().getSimpleName(), countType);
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
        return resultType;
    }

    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
        inputOperator.findDerivedTypes(derivedTypes);
        derivedTypes.add(resultType);
    }

    @Override
    public String describePlan()
    {
        return describePlan(inputOperator);
    }

    // Count_Default interface

    public Count_Default(Operator inputOperator, RowType countType, boolean usePValues)
    {
        ArgumentValidation.notNull("countType", countType);
        this.inputOperator = inputOperator;
        this.countType = countType;
        this.usePValues = usePValues;
        this.resultType = usePValues
            ? countType.schema().newValuesType(MNumeric.BIGINT.instance())
            : countType.schema().newValuesType(AkType.LONG);
    }
    
    // Class state

    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: Count_Default open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: Count_Default next");

    // Object state

    private final Operator inputOperator;
    private final RowType countType;
    private final ValuesRowType resultType;
    private final boolean usePValues;

    @Override
    public Explainer getExplainer()
    {
        return new CountOperatorExplainer("Count Default", countType, resultType, null);
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
                input.open();
                count = 0;
                closed = false;
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
                Row row = null;
                while ((row == null) && !closed) {
                    row = input.next();
                    if (row == null) {
                        close();
                        row = new ValuesRow(resultType, new Object[] { count });
                    } else if (row.rowType() == countType) {
                        row = null;
                        count++;
                    }
                }
                return row;
            } finally {
                TAP_NEXT.out();
            }
        }

        @Override
        public void close()
        {
            CursorLifecycle.checkIdleOrActive(this);
            if (!closed) {
                input.close();
                closed = true;
            }
        }

        @Override
        public void destroy()
        {
            close();
            input.destroy();
        }

        @Override
        public boolean isIdle()
        {
            return closed;
        }

        @Override
        public boolean isActive()
        {
            return !closed;
        }

        @Override
        public boolean isDestroyed()
        {
            return input.isDestroyed();
        }

        // Execution interface

        Execution(QueryContext context, Cursor input)
        {
            super(context);
            this.input = input;
        }

        // Object state

        private final Cursor input;
        private long count;
        private boolean closed = true;
    }
}
