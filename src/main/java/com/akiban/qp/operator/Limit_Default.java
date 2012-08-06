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
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.sql.optimizer.explain.Explainer;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.tap.InOutTap;
import com.akiban.server.error.NegativeLimitException;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.Extractors;

import com.akiban.sql.optimizer.explain.Attributes;
import com.akiban.sql.optimizer.explain.Label;
import com.akiban.sql.optimizer.explain.OperationExplainer;
import com.akiban.sql.optimizer.explain.PrimitiveExplainer;
import com.akiban.sql.optimizer.explain.Type;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**

 <h1>Overview</h1>

 Provides a very simple, count-based limit.

 <h1>Arguments</h1>

 <ul>

 <li><b>PhysicalOperator inputOperator:</b> The input operator, whose
 cursor will be limited

 <li><b>int limit:</b>&nbsp;the number of rows to limit to; cannot be
 negative

 </ul>

 <h1>Behavior</h1>

 Limit_Default's cursor returns at most <i>limit</i> rows from the input
 operator's cursor.  When the limit is reached, the input cursor is
 automatically closed, and all subsequent calls to <i>next</i> will return
 <i>false</i>.

 <h1>Output</h1>

 Nothing else to say.

 <h1>Assumptions</h1>

 None.

 <h1>Performance</h1>

 Essentially free.

 <h1>Memory Requirements</h1>

 None.

 */

final class Limit_Default extends Operator
{

    // Operator interface

    @Override
    protected Cursor cursor(QueryContext context) {
        return new Execution(context, inputOperator.cursor(context));
    }

    // Plannable interface

    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
        inputOperator.findDerivedTypes(derivedTypes);
    }

    @Override
    public List<Operator> getInputOperators() {
        return Collections.singletonList(inputOperator);
    }

    @Override
    public String describePlan() {
        return super.describePlan();
    }

    // Object interface

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder(getClass().getSimpleName());
        str.append("(");
        if (skip > 0) {
            str.append(String.format("skip=%d", skip));
        }
        if ((limit >= 0) && (limit < Integer.MAX_VALUE)) {
            if (skip > 0) str.append(", ");
            str.append(String.format("limit=%d", limit));
        }
        str.append(": ");
        str.append(inputOperator);
        str.append(")");
        return str.toString();
    }

    // Limit_Default interface

    Limit_Default(Operator inputOperator, int limit, boolean usePVals) {
        this(inputOperator, 0, false, limit, false, usePVals);
    }

    Limit_Default(Operator inputOperator,
                  int skip, boolean skipIsBinding,
                  int limit, boolean limitIsBinding,
                  boolean usePVals) {
        ArgumentValidation.isGTE("skip", skip, 0);
        ArgumentValidation.isGTE("limit", limit, 0);
        this.skip = skip;
        this.skipIsBinding = skipIsBinding;
        this.limit = limit;
        this.limitIsBinding = limitIsBinding;
        this.inputOperator = inputOperator;
        this.usePVals = usePVals;
    }

    public int skip() {
        return skip;
    }
    public boolean isSkipBinding() {
        return skipIsBinding;
    }
    public int limit() {
        return limit;
    }
    public boolean isLimitBinding() {
        return limitIsBinding;
    }
    
    // Class state
    
    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: Limit_Default open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: Limit_Default next");
    
    // Object state

    private final int skip, limit;
    private final boolean skipIsBinding, limitIsBinding;
    private final Operator inputOperator;
    private final boolean usePVals;

    @Override
    public Explainer getExplainer(Map<Object, Explainer> extraInfo)
    {
        Attributes atts = new Attributes();
        atts.put(Label.LIMIT, PrimitiveExplainer.getInstance(limit));
        
        return new OperationExplainer(Type.LIMIT_OPERATOR, atts);
    }

    // internal classes

    private class Execution extends ChainedCursor {

        // Cursor interface

        @Override
        public void open() {
            TAP_OPEN.in();
            try {
                CursorLifecycle.checkIdle(this);
                super.open();
                closed = false;
                if (isSkipBinding()) {
                    if (usePVals) {
                        PValueSource value = context.getPValue(skip());
                        if (!value.isNull())
                            this.skipLeft = value.getInt32();
                    }
                    else {
                        ValueSource value = context.getValue(skip());
                        if (!value.isNull())
                            this.skipLeft = (int)Extractors.getLongExtractor(AkType.LONG).getLong(value);
                    }
                }
                else {
                    this.skipLeft = skip();
                }
                if (skipLeft < 0)
                    throw new NegativeLimitException("OFFSET", skipLeft);
                if (isLimitBinding()) {
                    if (usePVals) {
                        PValueSource value = context.getPValue(limit());
                        if (value.isNull())
                            this.limitLeft = Integer.MAX_VALUE;
                        else
                            this.limitLeft = value.getInt32();
                    }
                    else {
                        ValueSource value = context.getValue(limit());
                        if (value.isNull())
                            this.limitLeft = Integer.MAX_VALUE;
                        else
                            this.limitLeft = (int)Extractors.getLongExtractor(AkType.LONG).getLong(value);
                    }
                }
                else {
                    this.limitLeft = limit();
                }
                if (limitLeft < 0)
                    throw new NegativeLimitException("LIMIT", limitLeft);
            } finally {
                TAP_OPEN.out();
            }
        }

        @Override
        public Row next() {
            TAP_NEXT.in();
            try {
                CursorLifecycle.checkIdleOrActive(this);
                checkQueryCancelation();
                Row row;
                while (skipLeft > 0) {
                    if ((row = input.next()) == null) {
                        skipLeft = 0;
                        limitLeft = -1;
                        close();
                        return null;
                    }
                    skipLeft--;
                }
                if (limitLeft < 0) {
                    close();
                    return null;
                }
                if (limitLeft == 0) {
                    close();
                    return null;
                }
                if ((row = input.next()) == null) {
                    limitLeft = -1;
                    close();
                    return null;
                }
                --limitLeft;
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
                super.close();
                closed = true;
            }
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

        // Execution interface
        Execution(QueryContext context, Cursor input) {
            super(context, input);
        }

        // object state

        private int skipLeft, limitLeft;
        private boolean closed = true;
    }
}
