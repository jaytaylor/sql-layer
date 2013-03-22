
package com.akiban.server.expression.std;

import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.*;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.sql.StandardException;

public class DegreesExpression extends AbstractUnaryExpression {

    @Scalar("degrees")
    public static final ExpressionComposer COMPOSER = new UnaryComposer() {

        @Override
        protected Expression compose(Expression argument, ExpressionType argType, ExpressionType resultType) {
            return new DegreesExpression(argument);
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException {
            if (argumentTypes.size() != 1) {
                throw new WrongExpressionArityException(1, argumentTypes.size());
            }
            argumentTypes.setType(0, AkType.DOUBLE);
            return ExpressionTypes.DOUBLE;
        }
    };

    protected DegreesExpression(Expression operand) {
        super(AkType.DOUBLE, operand);
    }

    @Override
    public String name() {
        return "DEGREES";
    }

    @Override
    public ExpressionEvaluation evaluation() {
        return new InnerEvaluation(operandEvaluation());
    }

    private static class InnerEvaluation extends AbstractUnaryExpressionEvaluation {

        public InnerEvaluation(ExpressionEvaluation eval) {
            super(eval);
        }

        @Override
        public ValueSource eval() {
            if (operand().isNull()) {
                return NullValueSource.only();
            }

            double degreeResult = Math.toDegrees(operand().getDouble());
            valueHolder().putDouble(degreeResult);

            return valueHolder();
        }
    }
}
