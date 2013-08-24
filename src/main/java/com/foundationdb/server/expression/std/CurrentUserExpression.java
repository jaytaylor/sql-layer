/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.expression.std;

import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.expression.ExpressionComposer;
import com.foundationdb.server.expression.ExpressionEvaluation;
import com.foundationdb.server.expression.ExpressionType;
import com.foundationdb.server.service.functions.Scalar;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.ValueSource;

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
