
package com.akiban.server.expression.std;

import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;

public class TruncateStringExpression extends AbstractUnaryExpression
{
    public TruncateStringExpression(int length, Expression operand) {
        super(AkType.VARCHAR, operand);
        this.length = length;
    }

    @Override
    public String name() {
        return "TRUNCATE_STRING";
    }

    @Override
    public String toString() {
        return name() + "(" + operand() + "," + length + ")";
    }

    @Override
    public ExpressionEvaluation evaluation() {
        return new InnerEvaluation(length, operandEvaluation());
    }

    private final int length;

    private static final class InnerEvaluation extends AbstractUnaryExpressionEvaluation {
        public InnerEvaluation(int length, ExpressionEvaluation ev) {
            super(ev);
            this.length = length;
        }

        @Override
        public ValueSource eval() {
            ValueSource operandSource = operand();
            if (operandSource.isNull())
                return NullValueSource.only();
            String value = operandSource.getString();
            if (value.length() > length)
                value = value.substring(0, length);
            valueHolder().putString(value);
            return valueHolder();          
        }

        private final int length;        
    }
}
