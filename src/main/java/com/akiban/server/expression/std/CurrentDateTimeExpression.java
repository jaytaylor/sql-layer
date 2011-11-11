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
import java.util.Calendar;
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
            = new DateTimeEnvironmentFactory(EnvironmentExpressionSetting.CURRENT_CALENDAR,  AkType.TIMESTAMP);

    /**
     * return now() expression  (an alias of current_timestamp())
     */
    @EnvironmentValue ("now")
    public static final EnvironmentExpressionFactory CURRENT_TIMESTAMP_ALIAS = CURRENT_TIMESTAMP_COMPOSER;

    private final AkType neededInfo;

    public CurrentDateTimeExpression (EnvironmentExpressionSetting environmentSetting, int bindingPos,
            AkType neededInfo)
    {
        super(checkType(neededInfo), environmentSetting, bindingPos);
        this.neededInfo = neededInfo;
    }

    private static final class InnerEvaluation extends EnvironmentEvaluation<Calendar>
    {
        private  AkType neededInfo;

        public InnerEvaluation (int bindingPos, AkType neededInfo)
        {
            super(bindingPos);
            this.neededInfo = neededInfo;
        }

        @Override
        public ValueSource eval()
        {  
            return new ValueHolder(neededInfo, new DateTime(this.environmentValue()));
        }
    }
    
    static class DateTimeEnvironmentFactory implements EnvironmentExpressionFactory 
    {
        private final EnvironmentExpressionSetting environmentSetting;
        private final AkType neededInfo;
        public DateTimeEnvironmentFactory (EnvironmentExpressionSetting environmentSetting,
               AkType neededInfo)
        {
            this.environmentSetting = environmentSetting;
            this.neededInfo = neededInfo;
        }

        @Override
        public EnvironmentExpressionSetting environmentSetting() 
        {
            return environmentSetting;
        }

        @Override
        public Expression get(int bindingPosition) 
        {
            return new CurrentDateTimeExpression(environmentSetting, bindingPosition, neededInfo);
        }
        
        @Override
        public ExpressionType getType() 
        {
            return ExpressionTypes.newType(neededInfo, 0, 0);
        }
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation (bindingPosition(), neededInfo);
    }

    @Override
    public String name ()
    {
        return "CURRENT_" + neededInfo;
    }
    
    private static AkType checkType (AkType input)
    {
        if (input == AkType.DATE  || input == AkType.TIME || input == AkType.TIMESTAMP) return input;
        else throw new UnsupportedOperationException("CURRENT_" + input + " is not supported");
    }
}
