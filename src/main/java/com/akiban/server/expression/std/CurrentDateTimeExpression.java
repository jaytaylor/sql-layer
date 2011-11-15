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

import com.akiban.server.expression.EnvironmentExpressionFactory;
import com.akiban.server.expression.EnvironmentExpressionSetting;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.expression.std.EnvironmentExpression.EnvironmentEvaluation;
import com.akiban.server.service.functions.EnvironmentValue;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;
import org.joda.time.DateTime;

public class CurrentDateTimeExpression extends EnvironmentExpression
{
    /**
     * return current_date() expression 
     */
    @EnvironmentValue("current_date")
    public static final EnvironmentExpressionFactory CURRENT_DATE_COMPOSER 
            = new DateTimeEnvironmentFactory(EnvironmentExpressionSetting.CURRENT_CALENDAR, AkType.DATE);
    
    /**
     * return current_time() expression 
     */
    @EnvironmentValue("current_time")
    public static final EnvironmentExpressionFactory CURRENT_TIME_COMPOSER 
            = new DateTimeEnvironmentFactory(EnvironmentExpressionSetting.CURRENT_CALENDAR, AkType.TIME);
    
    /**
     * return current_timestamp() expression in String
     */
    @EnvironmentValue("current_timestamp")
    public static final EnvironmentExpressionFactory CURRENT_TIMESTAMP_COMPOSER 
            = new DateTimeEnvironmentFactory(EnvironmentExpressionSetting.CURRENT_CALENDAR,  AkType.DATETIME);

    /**
     * return now() expression  (an alias of current_timestamp())
     */
    @EnvironmentValue ("now")
    public static final EnvironmentExpressionFactory CURRENT_TIMESTAMP_ALIAS = CURRENT_TIMESTAMP_COMPOSER;

    private final AkType currentType;

    public CurrentDateTimeExpression (EnvironmentExpressionSetting environmentSetting, int bindingPos,
            AkType currentType)
    {
        super(checkType(currentType), environmentSetting, bindingPos);
        this.currentType = currentType;
    }

    private static final class InnerEvaluation extends EnvironmentEvaluation<DateTime>
    {
        private  AkType currentType;

        public InnerEvaluation (int bindingPos, AkType currentType)
        {
            super(bindingPos);
            this.currentType = currentType;
        }

        @Override
        public ValueSource eval()
        {  
            return new ValueHolder(currentType, environmentValue());
        }
    }
    
    static class DateTimeEnvironmentFactory implements EnvironmentExpressionFactory 
    {
        private final EnvironmentExpressionSetting environmentSetting;
        private final AkType currentType;
        public DateTimeEnvironmentFactory (EnvironmentExpressionSetting environmentSetting,
               AkType currentType)
        {
            this.environmentSetting = environmentSetting;
            this.currentType = currentType;
        }

        @Override
        public EnvironmentExpressionSetting environmentSetting() 
        {
            return environmentSetting;
        }

        @Override
        public Expression get(int bindingPosition) 
        {
            return new CurrentDateTimeExpression(environmentSetting, bindingPosition, currentType);
        }
        
        @Override
        public ExpressionType getType() 
        {
            return ExpressionTypes.newType(currentType, 0, 0);
        }
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation (bindingPosition(), currentType);
    }

    @Override
    public String name ()
    {
        return "CURRENT_" + currentType;
    }
    
    private static AkType checkType (AkType input)
    {
        if (input == AkType.DATE  || input == AkType.TIME || input == AkType.DATETIME) return input;
        else throw new UnsupportedOperationException("CURRENT_" + input + " is not supported");
    }
}
