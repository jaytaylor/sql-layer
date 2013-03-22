
package com.akiban.server.expression.std;

import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.util.BoolValueSource;
import com.akiban.sql.StandardException;
import com.akiban.server.expression.TypesList;

public final class NotExpression extends AbstractUnaryExpression {

    @Scalar("not") public static final ExpressionComposer COMPOSER = new UnaryComposer() {
        @Override
        protected Expression compose(Expression argument, ExpressionType argType, ExpressionType resultType) {
            return new NotExpression(argument);
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 1)
                throw new WrongExpressionArityException(1, argumentTypes.size());
            return ExpressionTypes.BOOL;
        }
    };

    @Override
    public String name() {
        return "NOT";
    }

    @Override
    public ExpressionEvaluation evaluation() {
        return new InnerEvaluation(operandEvaluation());
    }

    public NotExpression(Expression operand) {
        super(AkType.BOOL, operand);
    }

    private static class InnerEvaluation extends AbstractUnaryExpressionEvaluation {

        @Override
        public ValueSource eval() {
            Boolean answer = Extractors.getBooleanExtractor().getBoolean(operand(), null);
            if (answer != null)
                answer = !answer;
            return BoolValueSource.of(answer);
        }

        private InnerEvaluation(ExpressionEvaluation operandEvaluation) {
            super(operandEvaluation);
        }
    }
}
