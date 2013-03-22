
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

    @Override
    public boolean usePValues() {
        return pExpressions != null;
    }

    /* Object */

    @Override
    public String toString() {
        String exprs = (pExpressions != null) ? pExpressions.toString() : expressions.toString();
        return getClass().getSimpleName() + "(" + exprs + ")";
    }

}
