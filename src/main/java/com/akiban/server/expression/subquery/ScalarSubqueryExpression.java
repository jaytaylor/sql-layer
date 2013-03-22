
package com.akiban.server.expression.subquery;

import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.error.SubqueryTooManyRowsException;
import com.akiban.server.explain.*;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;

public final class ScalarSubqueryExpression extends SubqueryExpression {

    @Override
    public ExpressionEvaluation evaluation() {
        return new InnerEvaluation(subquery(), expression.evaluation(),
                                   outerRowType(), innerRowType(),
                                   bindingPosition());
    }

    @Override
    public AkType valueType() {
        return expression.valueType();
    }

    @Override
    public String toString() {
        return "VALUE(" + subquery() + ")";
    }

    @Override
    public String name () {
        return "VALUE";
    }
    
    @Override
    public CompoundExplainer getExplainer(ExplainContext context) {
        CompoundExplainer explainer = super.getExplainer(context);
        explainer.addAttribute(Label.EXPRESSIONS, expression.getExplainer(context));
        return explainer;
    }

    public ScalarSubqueryExpression(Operator subquery, Expression expression,
                                    RowType outerRowType, RowType innerRowType, 
                                    int bindingPosition) {
        super(subquery, outerRowType, innerRowType, bindingPosition);
        this.expression = expression;
    }
                                 
    private final Expression expression;

    private static final class InnerEvaluation extends SubqueryExpressionEvaluation {
        @Override
        public ValueSource doEval() {
            Row row = next();
            if (row == null)
                return NullValueSource.only();
            expressionEvaluation.of(queryContext());
            expressionEvaluation.of(row);
            // Make a copy of the value evaluated right now, rather
            // than holding on to the row from the subquery cursor
            // that's about to be advanced and closed.
            ValueSource result = new ValueHolder(expressionEvaluation.eval());
            if (next() != null)
                throw new SubqueryTooManyRowsException();
            return result;
        }

        private InnerEvaluation(Operator subquery,
                                ExpressionEvaluation expressionEvaluation, 
                                RowType outerRowType, RowType innerRowType, 
                                int bindingPosition) {
            super(subquery, outerRowType, innerRowType, bindingPosition);
            this.expressionEvaluation = expressionEvaluation;
        }

        private final ExpressionEvaluation expressionEvaluation;
    }

}
