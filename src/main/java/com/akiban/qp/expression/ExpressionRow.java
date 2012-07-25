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

import java.util.ArrayList;
import java.util.List;

import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.AbstractRow;
import com.akiban.qp.row.HKey;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.texpressions.TEvaluatableExpression;
import com.akiban.server.types3.texpressions.TPreparedExpression;

public class ExpressionRow extends AbstractRow
{
    private RowType rowType;
    private List<? extends Expression> expressions;
    private List<ExpressionEvaluation> evaluations;
    private List<? extends TPreparedExpression> pExpressions;
    private List<TEvaluatableExpression> pEvaluations;

    public ExpressionRow(RowType rowType, QueryContext context, List<? extends Expression> expressions,
                         List<? extends TPreparedExpression> pExpressions) {
        this.rowType = rowType;
        this.expressions = expressions;
        this.pExpressions = pExpressions;
        if (pExpressions != null) {
            assert expressions == null : "can't have both types be non-null";
            this.pEvaluations = new ArrayList<TEvaluatableExpression>(pExpressions.size());
            for (TPreparedExpression expression : pExpressions) {
                TEvaluatableExpression evaluation = expression.build(context);
                evaluation.with(context);
                this.pEvaluations.add(evaluation);
            }
        }
        else if (expressions != null) {
            this.evaluations = new ArrayList<ExpressionEvaluation>(expressions.size());
            for (Expression expression : expressions) {
                if (expression.needsRow()) {
                    throw new AkibanInternalException("expression needed a row: " + expression + " in " + expressions);
                }
                ExpressionEvaluation evaluation = expression.evaluation();
                evaluation.of(context);
                this.evaluations.add(evaluation);
            }
        }
        else
            throw new AssertionError("can't have both lists be null");
    }

    /* AbstractRow */

    @Override
    public RowType rowType() {
        return rowType;
    }

    @Override
    public ValueSource eval(int i) {
        return evaluations.get(i).eval();
    }

    @Override
    public PValueSource pvalue(int i) {
        TEvaluatableExpression eval = pEvaluations.get(i);
        eval.evaluate();
        return eval.resultValue();
    }

    @Override
    public HKey hKey() {
        throw new UnsupportedOperationException();        
    }

    @Override
    public void release() {
    }

    @Override
    public boolean isShared() {
        return false;
    }

    @Override
    public void acquire() {
    }

    /* Object */

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append(getClass().getSimpleName());
        str.append('[');
        int nf = rowType().nFields();
        for (int i = 0; i < nf; i++) {
            if (i > 0) str.append(", ");
            Object expression = (pExpressions != null) ? pExpressions.get(i) : expressions.get(i);
            if (expression != null)
                str.append(expression);
        }
        str.append(']');
        return str.toString();
    }
}
