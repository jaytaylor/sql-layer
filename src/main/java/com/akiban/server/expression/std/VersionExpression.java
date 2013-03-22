
package com.akiban.server.expression.std;

import com.akiban.server.AkServer;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;

public abstract class VersionExpression extends AbstractNoArgExpression
{
    @Scalar("version_full")
    public static final ExpressionComposer FULL_COMPOSER = new NoArgComposer()
    {
        @Override
        protected Expression compose()
        {
            return FULL_VERSION;
        }

        @Override
        protected ExpressionType composeType()
        {
            return ExpressionTypes.varchar(FULL_VERSION_SOURCE.getString().length());
        }
    };

    @Scalar("version")
    public static final ExpressionComposer SHORT_COMPOSER = new NoArgComposer()
    {
        @Override
        protected Expression compose()
        {
            return SHORT_VERSION;
        }

        @Override
        protected ExpressionType composeType()
        {
            return ExpressionTypes.varchar(SHORT_VERSION_SOURCE.getString().length());
        }
    };
    
    private VersionExpression()
    {
        super(AkType.VARCHAR);
    }

    @Override
    public String name()
    {
        return "version";
    }
    
    // static members

    private static final ValueSource FULL_VERSION_SOURCE = new ValueHolder(AkType.VARCHAR, AkServer.VERSION_STRING);
    private static final ValueSource SHORT_VERSION_SOURCE = new ValueHolder(AkType.VARCHAR, AkServer.SHORT_VERSION_STRING);
    
    private static final Expression FULL_VERSION = new VersionExpression()
    {
        @Override
        public ExpressionEvaluation evaluation()
        {
            return FULL_EVAL;
        }
    };
    
    private static final Expression SHORT_VERSION = new VersionExpression()
    {
        @Override
        public ExpressionEvaluation evaluation()
        {
            return SHORT_EVAL;
        }
    };
    
    private static final ExpressionEvaluation FULL_EVAL = new AbstractNoArgExpressionEvaluation()
    {
        @Override
        public ValueSource eval()
        {
            return FULL_VERSION_SOURCE;
        }
    };
    
    private static final ExpressionEvaluation SHORT_EVAL = new AbstractNoArgExpressionEvaluation()
    {
        @Override
        public ValueSource eval()
        {
            return SHORT_VERSION_SOURCE;
        }   
    };
}
