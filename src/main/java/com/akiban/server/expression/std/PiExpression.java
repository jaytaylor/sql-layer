
package com.akiban.server.expression.std;

import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;

public class PiExpression extends AbstractNoArgExpression
{
    @Scalar("pi")
    public static final ExpressionComposer COMPOSER = new NoArgComposer()
    {
        @Override
        protected Expression compose()
        {
            return INSTANCE;
        }

        @Override
        protected ExpressionType composeType()
        {
            return ExpressionTypes.DOUBLE;
        }
        
    };
    
    private static final class InnerEvaluation extends AbstractNoArgExpressionEvaluation
    {
        private static final ValueSource pi = new ValueHolder(AkType.DOUBLE, Math.PI);
        
        @Override
        public ValueSource eval()
        {
            return pi;
        }
    }

    @Override
    public String name()
    {
        return "PI";
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return EVALUATION;
    }
    
        
    protected PiExpression ()
    {
        super(AkType.DOUBLE);
    }
    
    private static final ExpressionEvaluation EVALUATION = new InnerEvaluation();
    private static final Expression INSTANCE = new PiExpression();
}
