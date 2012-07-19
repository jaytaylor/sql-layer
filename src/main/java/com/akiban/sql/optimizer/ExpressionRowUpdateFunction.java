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

package com.akiban.sql.optimizer;

import java.util.List;

import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.operator.UpdateFunction;
import com.akiban.qp.row.OverlayingRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types3.texpressions.TEvaluatableExpression;
import com.akiban.server.types3.texpressions.TPreparedExpression;

/** Update a row by substituting expressions for some fields. */
public class ExpressionRowUpdateFunction implements UpdateFunction
{
    private final List<Expression> expressions;
    protected final List<TPreparedExpression> pExpressions;
    private final RowType rowType;

    public ExpressionRowUpdateFunction(List<Expression> expressions, List<TPreparedExpression> pExpressions, RowType rowType) {
        this.expressions = expressions;
        this.pExpressions = pExpressions;
        this.rowType = rowType;
    }

    /* UpdateFunction */

    @Override
    public boolean rowIsSelected(Row row) {
        return row.rowType().equals(rowType);
    }

    @Override
    public Row evaluate(Row original, QueryContext context) {
        OverlayingRow result = new OverlayingRow(original, pExpressions != null);
        int nfields = rowType.nFields();
        for (int i = 0; i < nfields; i++) {
            if (pExpressions != null) {
                assert expressions == null : "can't have both expression types";
                TPreparedExpression expression = pExpressions.get(i);
                if (expression != null) {
                    TEvaluatableExpression evaluation = expression.build();
                    evaluation.with(original);
                    evaluation.with(context);
                    evaluation.evaluate();
                    result.overlay(i, evaluation.resultValue());
                }
            }
            else if (expressions != null) {
                Expression expression = expressions.get(i);
                if (expression != null) {
                    ExpressionEvaluation evaluation = expression.evaluation();
                    evaluation.of(original);
                    evaluation.of(context);
                    result.overlay(i, evaluation.eval());
                }
            }
            else {
                assert false: "must have one expression list";
            }
        }
        return result;
    }

    /* Object */

    @Override
    public String toString() {
        String exprs = (pExpressions != null) ? pExpressions.toString() : expressions.toString();
        return getClass().getSimpleName() + "(" + exprs + ")";
    }

}
