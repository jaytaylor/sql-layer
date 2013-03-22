
package com.akiban.server.expression.subquery;

import com.akiban.qp.operator.Operator;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.BoolValueSource;

public final class ExistsSubqueryExpression extends SubqueryExpression {

    @Override
    public ExpressionEvaluation evaluation() {
        return new InnerEvaluation(subquery(), outerRowType(), innerRowType(),
                                   bindingPosition());
    }

    @Override
    public AkType valueType() {
        return AkType.BOOL;
    }

    @Override
    public String toString() {
        return "EXISTS(" + subquery() + ")";
    }

    public ExistsSubqueryExpression(Operator subquery, RowType outerRowType, 
                                    RowType innerRowType, int bindingPosition) {
        super(subquery, outerRowType, innerRowType, bindingPosition);
    }

    @Override
    public String name()
    {
        return "EXISTS";
    }
                                 
    private static final class InnerEvaluation extends SubqueryExpressionEvaluation {
        @Override
        public ValueSource doEval() {
            boolean empty = (next() == null);
            return BoolValueSource.of(!empty);
        }

        private InnerEvaluation(Operator subquery,
                                RowType outerRowType, RowType innerRowType, 
                                int bindingPosition) {
            super(subquery, outerRowType, innerRowType, bindingPosition);
        }
    }

}
