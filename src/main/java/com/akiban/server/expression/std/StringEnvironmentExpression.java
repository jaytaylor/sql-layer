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
import com.akiban.server.expression.EnvironmentExpressionFactory;
import com.akiban.server.expression.EnvironmentExpressionSetting;
import com.akiban.server.service.functions.EnvironmentValue;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;

public class StringEnvironmentExpression extends EnvironmentExpression
{
     @EnvironmentValue("current_user")
     public static final EnvironmentExpressionFactory CURRENT_USER = 
         new StringEnvironmentFactory(EnvironmentExpressionSetting.CURRENT_USER);
 
     @EnvironmentValue("session_user")
     public static final EnvironmentExpressionFactory SESSION_USER = 
         new StringEnvironmentFactory(EnvironmentExpressionSetting.SESSION_USER);
 
     @EnvironmentValue("system_user")
     public static final EnvironmentExpressionFactory SYSTEM_USER = 
         new StringEnvironmentFactory(EnvironmentExpressionSetting.SYSTEM_USER);
     
     static class StringEnvironmentFactory implements EnvironmentExpressionFactory {
         @Override
         public EnvironmentExpressionSetting environmentSetting() {
             return environmentSetting;
         }

         @Override
         public Expression get(int bindingPosition) {
             return new StringEnvironmentExpression(environmentSetting, bindingPosition);
         }

         @Override
         public ExpressionType getType() {
             return ExpressionTypes.varchar(128);
         }
 
         public StringEnvironmentFactory(EnvironmentExpressionSetting environmentSetting) {
             this.environmentSetting = environmentSetting;
         }

         private EnvironmentExpressionSetting environmentSetting;
     }

    protected StringEnvironmentExpression(EnvironmentExpressionSetting environmentSetting,
                                          int bindingPosition) {
        super(AkType.VARCHAR, environmentSetting, bindingPosition);
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
