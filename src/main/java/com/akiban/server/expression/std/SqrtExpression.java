
package com.akiban.server.expression.std;

import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.*;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.sql.StandardException;

public class SqrtExpression extends AbstractUnaryExpression {
    
    @Scalar("sqrt")
    public static final ExpressionComposer COMPOSER = new InternalComposer();
    
    private static class InternalComposer extends UnaryComposer
    {

        @Override
        protected Expression compose(Expression argument, ExpressionType argType, ExpressionType resultType)
        {
            return new SqrtExpression(argument);
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 1)
                throw new WrongExpressionArityException(1, argumentTypes.size());

            argumentTypes.setType(0, AkType.DOUBLE);
            return ExpressionTypes.DOUBLE;
        }
  
    }
    
    private static class InnerEvaluation extends AbstractUnaryExpressionEvaluation
    {

        @Override
        public ValueSource eval()
        {
            if (operand().isNull())
                return NullValueSource.only();
            
            if (operand().getDouble() < 0)
            {
                valueHolder().putDouble(Double.NaN);
                return valueHolder();
            }
            
            double sqrtResult = Math.sqrt(operand().getDouble());
            valueHolder().putDouble(sqrtResult);

            return valueHolder();
        }
        
        public InnerEvaluation(ExpressionEvaluation eval)
        {
            super(eval);
        }
        
    }
    
    @Override
    public String name()
    {
        return "sqrt";
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(this.operandEvaluation());
    }
    
    protected SqrtExpression(Expression operand)
    {
        super(AkType.DOUBLE, operand);
    }

}
