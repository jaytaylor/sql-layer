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

import com.akiban.qp.exec.Plannable;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.sql.optimizer.explain.Explainer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TimeOperator extends Operator
{
    // Object interface

    @Override
    public String toString()
    {
        return getClass().getSimpleName();
    }

    // Operator interface

    @Override
    protected Cursor cursor(QueryContext context)
    {
        return new Execution(context);
    }

    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
        inputOperator.findDerivedTypes(derivedTypes);
    }

    @Override
    public List<Operator> getInputOperators()
    {
        List<Operator> result = new ArrayList<Operator>(1);
        result.add(inputOperator);
        return result;
    }

    @Override
    public String describePlan()
    {
        return describePlan(inputOperator);
    }

    // TimeOperator interface

    public long elapsedNsec()
    {
        return elapsedNsec;
    }

    public TimeOperator(Operator inputOperator)
    {
        this.inputOperator = inputOperator;
    }

    // Object state

    private final Operator inputOperator;
    private long elapsedNsec = 0;

    @Override
    public Explainer getExplainer(Map<Object, Explainer> extraInfo) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    // Inner classes

    private class Execution extends OperatorExecutionBase implements Cursor
    {
        // Cursor interface

        @Override
        public void open()
        {
            long start = System.nanoTime();
            input.open();
            long stop = System.nanoTime();
            elapsedNsec += stop - start;
        }

        @Override
        public Row next()
        {
            long start = System.nanoTime();
            Row next = input.next();
            long stop = System.nanoTime();
            elapsedNsec += stop - start;
            return next;
        }

        @Override
        public void close()
        {
            long start = System.nanoTime();
            input.close();
            long stop = System.nanoTime();
            elapsedNsec += stop - start;
        }

        @Override
        public void destroy()
        {
            input.destroy();
        }

        @Override
        public boolean isIdle()
        {
            return input.isIdle();
        }

        @Override
        public boolean isActive()
        {
            return input.isActive();
        }

        @Override
        public boolean isDestroyed()
        {
            return input.isDestroyed();
        }

        // Execution interface

        Execution(QueryContext context)
        {
            super(context);
            this.input = inputOperator.cursor(context);
        }

        // Object state

        private final Cursor input;
    }
}
