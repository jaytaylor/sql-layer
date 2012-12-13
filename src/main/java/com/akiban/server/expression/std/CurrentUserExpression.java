/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

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
