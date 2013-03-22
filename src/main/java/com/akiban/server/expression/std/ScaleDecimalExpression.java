
package com.akiban.server.expression.std;

import com.akiban.server.error.OverflowException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class ScaleDecimalExpression extends AbstractUnaryExpression
{
    public ScaleDecimalExpression(int precision, int scale, Expression operand) {
        super(AkType.DECIMAL, operand);
        this.precision = precision;
        this.scale = scale;
    }

    @Override
    public String name() {
        return "SCALE_DECIMAL";
    }

    @Override
    public String toString() {
        return name() + "(" + operand() + "," + precision + "." + scale + ")";
    }

    @Override
    public ExpressionEvaluation evaluation() {
        return new InnerEvaluation(precision, scale, RoundingMode.UP, 
                                   operandEvaluation());
    }

    private final int precision, scale;

    private static final class InnerEvaluation extends AbstractUnaryExpressionEvaluation {
        public InnerEvaluation(int precision, int scale, RoundingMode roundingMode,
                               ExpressionEvaluation ev) {
            super(ev);
            this.precision = precision;
            this.scale = scale;
            this.roundingMode = roundingMode;
        }

        @Override
        public ValueSource eval() {
            
            ValueSource operandSource = operand();
            if (operandSource.isNull())
                return NullValueSource.only();
            BigDecimal value = operandSource.getDecimal();
            value = value.setScale(scale, roundingMode);
            if (value.precision() > precision)
                throw new OverflowException();
            valueHolder().putDecimal(value);
            return valueHolder();
        }

        private final int precision, scale;
        private final RoundingMode roundingMode;            
    }
}
