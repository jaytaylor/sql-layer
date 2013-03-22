
package com.akiban.server.expression.std;

import com.akiban.qp.operator.QueryContext;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;

public abstract class CurrentUserExpression extends AbstractNoArgExpression
{
    private final String name;

    @Scalar("current_user")
    public static final ExpressionComposer CURRENT_USER = 
        new UserComposer() {
            @Override
            protected Expression compose() {
                return new CurrentUserExpression("CURRENT_USER") {
                        @Override
                        public String environmentValue(QueryContext context) {
                            return context.getCurrentUser();
                        }
                    };
            }
        };
 
    @Scalar("session_user")
    public static final ExpressionComposer SESSION_USER = 
        new UserComposer() {
            @Override
            protected Expression compose() {
                return new CurrentUserExpression("SESSION_USER") {
                        @Override
                        public String environmentValue(QueryContext context) {
                            return context.getSessionUser();
                        }
                    };
            }
        };
 
    @Scalar("system_user")
    public static final ExpressionComposer SYSTEM_USER = 
        new UserComposer() {
            @Override
            protected Expression compose() {
                return new CurrentUserExpression("SYSTEM_USER") {
                        @Override
                        public String environmentValue(QueryContext context) {
                            return context.getSystemUser();
                        }
                    };
            }
        };
     
    @Scalar("current_schema")
    public static final ExpressionComposer CURRENT_SCHEMA = 
        new UserComposer() {
            @Override
            protected Expression compose() {
                return new CurrentUserExpression("CURRENT_SCHEMA") {
                        @Override
                        public String environmentValue(QueryContext context) {
                            return context.getCurrentSchema();
                        }
                    };
            }
        };
 
    public abstract String environmentValue(QueryContext context);

    public CurrentUserExpression(String name)
    {
        super(AkType.VARCHAR);
        this.name = name;
    }
    
    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean isConstant() {
        return true;
    }

    @Override
    public boolean needsBindings() {
        return true;
    }

    private static abstract class UserComposer extends NoArgComposer {
        @Override
        protected ExpressionType composeType()
        {
            return ExpressionTypes.varchar(128);
        }        
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation();
    }

    private final class InnerEvaluation extends AbstractNoArgExpressionEvaluation
    {
        private QueryContext context;

        public InnerEvaluation() {
        }

        @Override
        public void of(QueryContext context) {
            this.context = context;
        }
        
        @Override
        public ValueSource eval() 
        {
            valueHolder().putString(environmentValue(context));
            return valueHolder();
        }
    }
}
