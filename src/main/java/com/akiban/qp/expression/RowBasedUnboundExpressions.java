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

package com.akiban.qp.expression;

import com.akiban.qp.operator.API;
import com.akiban.qp.operator.ExpressionGenerator;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.explain.Attributes;
import com.akiban.server.explain.CompoundExplainer;
import com.akiban.server.explain.ExplainContext;
import com.akiban.server.explain.Label;
import com.akiban.server.explain.Type;
import com.akiban.server.expression.Expression;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.texpressions.TPreparedExpression;

import java.util.List;

public final class RowBasedUnboundExpressions implements UnboundExpressions {
    @Override
    public BoundExpressions get(QueryContext context) {
        return new ExpressionsAndBindings(rowType, expressions, pExprs, context);
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context) {
        Attributes atts = new Attributes();
        if (pExprs != null) {
            for (TPreparedExpression expression : pExprs) {
                atts.put(Label.EXPRESSIONS, expression.getExplainer(context));
            }
        }
        else {
            for (Expression expression : expressions) {
                atts.put(Label.EXPRESSIONS, expression.getExplainer(context));
            }
        }
        return new CompoundExplainer(Type.ROW, atts);
    }

    @Override
    public String toString() {
        return "UnboundExpressions" + (expressions == null ? pExprs : expressions);
    }

    @Deprecated
    public RowBasedUnboundExpressions(RowType rowType, List<ExpressionGenerator> expressions) {
        this(rowType, API.generateOld(expressions), API.generateNew(expressions));
    }

    public RowBasedUnboundExpressions(RowType rowType, List<Expression> expressions, List<TPreparedExpression> pExprs)
    {
        if (expressions != null) {
            assert pExprs == null : "both can't be non-null";
            for (Expression expression : expressions) {
                if (expression == null) {
                    throw new IllegalArgumentException();
                }
            }
        }
        else if (pExprs != null) {
            for (TPreparedExpression expression : pExprs) {
                if (expression == null) {
                    throw new IllegalArgumentException();
                }
            }
        }
        else
            assert false : "both can't be null";
        this.expressions = expressions;
        this.pExprs = pExprs;
        this.rowType = rowType;
    }

    private final List<Expression> expressions;
    private final List<TPreparedExpression> pExprs;
    private final RowType rowType;

    private static class ExpressionsAndBindings implements BoundExpressions {

        @Override
        public ValueSource eval(int index) {
            return expressionRow.eval(index);
        }

        @Override
        public PValueSource pvalue(int index) {
            return expressionRow.pvalue(index);
        }

        @Override
        public int compareTo(BoundExpressions row, int leftStartIndex, int rightStartIndex, int fieldCount)
        {
            throw new UnsupportedOperationException();
        }

        ExpressionsAndBindings(RowType rowType, List<Expression> expressions, List<TPreparedExpression> pExprs,
                               QueryContext context)
        {
            expressionRow = new ExpressionRow(rowType, context, expressions, pExprs);
        }

        private final ExpressionRow expressionRow;
    }
}
