
package com.akiban.server.expression.std;

import com.akiban.qp.operator.QueryContext;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.*;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.sql.StandardException;
import com.akiban.util.ByteSource;
import com.akiban.util.Strings;
import com.akiban.util.WrappingByteSource;

public class UnhexExpression extends AbstractUnaryExpression
{
    @Scalar("unhex")
    public static final ExpressionComposer COMPOSER = new UnaryComposer()
    {
        @Override
        protected Expression compose(Expression argument, ExpressionType argType, ExpressionType resultType)
        {
            return new UnhexExpression(argument);
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 1)
                throw new WrongExpressionArityException(1, argumentTypes.size());
            
            argumentTypes.setType(0, AkType.VARCHAR);            
            return ExpressionTypes.varbinary(argumentTypes.get(0).getPrecision() / 2 + 1);
        }
        
    };
    
    private static class InnerEvaluation extends AbstractUnaryExpressionEvaluation
    {
        private static final ByteSource EMPTY = new WrappingByteSource(new byte[0]);
        
        InnerEvaluation(ExpressionEvaluation arg)
        {
            super(arg);
        }
       
        @Override
        public ValueSource eval()
        {
            ValueSource source = operand();
            if (source.isNull())
                return NullValueSource.only();
            
            String st = source.getString();
            if (st.isEmpty())
                valueHolder().putVarBinary(EMPTY);
            else
                try
                {
                    valueHolder().putVarBinary(Strings.parseHexWithout0x(st));
                }
                catch (InvalidOperationException e)
                {
                    QueryContext qc = queryContext();
                    if (qc != null)
                        qc.warnClient(e);
                    return NullValueSource.only();
                }
            
            return valueHolder();
        }
    }
    
    UnhexExpression (Expression arg)
    {
        super(AkType.VARBINARY, arg);
    }
    
    @Override
    public String name()
    {
        return "UNHEX";
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(operandEvaluation());
    }
}
