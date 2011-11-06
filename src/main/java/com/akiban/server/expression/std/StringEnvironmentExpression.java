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

import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;

public class StringEnvironmentExpression extends EnvironmentExpression
{
    @Scalar("current_user")
    public static final ExpressionComposer CURRENT_USER = 
        new StringEnvironmentComposer(EnvironmentValue.CURRENT_USER);

    @Scalar("session_user")
    public static final ExpressionComposer SESSION_USER = 
        new StringEnvironmentComposer(EnvironmentValue.SESSION_USER);

    @Scalar("system_user")
    public static final ExpressionComposer SYSTEM_USER = 
        new StringEnvironmentComposer(EnvironmentValue.SYSTEM_USER);
    
    static class StringEnvironmentComposer extends EnvironmentComposer {
        @Override
        protected ExpressionType composeType() {
            return ExpressionTypes.varchar(128);
        }

        @Override
        protected Expression compose(int bindingPosition) {
            return new StringEnvironmentExpression(environmentValue(), bindingPosition);
        }

        public StringEnvironmentComposer(EnvironmentValue environmentValue) {
            super(environmentValue);
        }
    }

    public StringEnvironmentExpression(EnvironmentValue environmentValue,
                                       int bindingPosition) {
        super(AkType.VARCHAR, environmentValue, bindingPosition);
    }

    @Override
    public ExpressionEvaluation evaluation() 
    {
        return new InnerEvaluation(bindingPosition());
    }

    private static final class InnerEvaluation extends EnvironmentEvaluation
    {
        public InnerEvaluation(int bindingsPosition) {
            super(bindingsPosition);
        }

        @Override
        public ValueSource eval() 
        {
            return new ValueHolder(AkType.VARCHAR, (String)environmentValue());
        }
    }
}
