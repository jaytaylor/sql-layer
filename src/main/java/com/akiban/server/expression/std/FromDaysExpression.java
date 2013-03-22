
package com.akiban.server.expression.std;

import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.expression.TypesList;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.Extractors;
import com.akiban.sql.StandardException;
import org.joda.time.DateTimeZone;


public class FromDaysExpression extends AbstractUnaryExpression
{
    @Scalar("FROM_DAYS")
    public static final ExpressionComposer COMPOSER = new UnaryComposer()
    {
        @Override
        protected Expression compose(Expression argument, ExpressionType argType, ExpressionType resultType)
        {
            return new FromDaysExpression(argument);
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 1)
                throw new WrongExpressionArityException(1, argumentTypes.size());
            
            argumentTypes.setType(0, AkType.LONG);
            return ExpressionTypes.DATE;
        }
        
    };
    
    private static class InnerEvaluation extends AbstractUnaryExpressionEvaluation
    {
        private static final long BEGINNING = Extractors.getLongExtractor(AkType.DATE).stdLongToUnix(33, DateTimeZone.UTC);
        private  static final long FACTOR = 3600L * 1000 * 24;
        
        InnerEvaluation (ExpressionEvaluation eval)
        {
            super(eval);
        }
        
        @Override
        public ValueSource eval()
        {
            ValueSource days = operand();
            if (days.isNull())
                return NullValueSource.only();
            
            long val = days.getLong();
            valueHolder().putDate( val < 366
                    ? 0L
                    :Extractors.getLongExtractor(AkType.DATE).unixToStdLong(days.getLong() * FACTOR + BEGINNING, DateTimeZone.UTC));
            return valueHolder();
        }    
    }
    
    FromDaysExpression (Expression arg)
    {
        super(AkType.DATE, arg);
    }
    
    @Override
    public String name()
    {
        return "FROM_DAYS";
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(operandEvaluation());
    }
    
}
