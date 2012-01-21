/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
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
    @Scalar("current_user")
    public static final ExpressionComposer CURRENT_USER = 
        new UserComposer() {
            @Override
            protected Expression compose() {
                return new CurrentUserExpression() {
                        @Override
                        public abstract String environmentValue(QueryContext context) {
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
                return new CurrentUserExpression(QueryContext context) {
                        @Override
                        public abstract String environmentValue() {
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
                return new CurrentUserExpression(QueryContext context) {
                        @Override
                        public abstract String environmentValue() {
                            return context.getSystemUser();
                        }
                    };
            }
        };
     
    public abstract String environmentValue(QueryContext context);

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
        return new InnerEvaluation(this);
    }

    private final class InnerEvaluation extends AbstractNoArgExpressionEvaluation
    {
        private QueryContext context;

        public InnerEvaluation(QueryContext context) {
        }

        @Override
        public void of(QueryContext context) {
            this.context = context;
        }
        
        @Override
        public ValueSource eval() 
        {
            valueHolder().putString(environmentValue());
            return valueHolder();
        }
    }
}
