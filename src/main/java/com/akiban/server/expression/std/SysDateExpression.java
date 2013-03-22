
package com.akiban.server.expression.std;

import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;


public class SysDateExpression extends AbstractNoArgExpression
{
    @Scalar("sysdate")
    public static final ExpressionComposer COMPOSER = new NoArgComposer()
    {
        @Override
        protected Expression compose()
        {
            return new SysDateExpression();
        }

        @Override
        protected ExpressionType composeType()
        {
            return ExpressionTypes.TIMESTAMP;
        }        
    };
    
    private static class InnerEvaluation extends AbstractNoArgExpressionEvaluation
    {
        @Override
        public ValueSource eval() 
        {
            valueHolder().putTimestamp(new DateTime(DateTimeZone.getDefault()));
            return valueHolder();
        }        
    }
    
    public SysDateExpression ()
    {
        super(AkType.TIMESTAMP);
    }
    
    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public String name() 
    {
        return "SYSDATE";
    }

    @Override
    public ExpressionEvaluation evaluation() 
    {
        return new InnerEvaluation();
    }
    
}
