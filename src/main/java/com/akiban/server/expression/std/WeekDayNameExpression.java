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

import com.akiban.server.error.InvalidArgumentTypeException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.util.ValueHolder;
import org.joda.time.DateTime;

public class WeekDayNameExpression extends AbstractUnaryExpression
{
    protected static enum Field
    {
        DAYNAME, DAYOFWEEK, WEEKDAY
    }

    /**
     * Returns the day name
     * Eg., DAYNAME("2011-12-05") --> Monday
     */
    @Scalar("dayname")
    public static final ExpressionComposer DAYNAME_COMPOSER = new InternalComposer(Field.DAYNAME);

    /**
     * Returns the weekday index for date (1 = Sunday, 2 = Monday, …, 7 = Saturday).
     * These index values correspond to the ODBC standard.
     */
    @Scalar("dayofweek")
    public static final ExpressionComposer DAYOFWEEK_COMPOSER = new InternalComposer(Field.DAYOFWEEK);

    /**
     * Returns the weekday index for date (0 = Monday, 1 = Tuesday, … 6 = Sunday).
     */
    @Scalar("weekday")
    public static final ExpressionComposer WEEKDAY_COMPOSER = new InternalComposer(Field.WEEKDAY);
    
    private static class InternalComposer extends UnaryComposer
    {
        private final Field field;

        public InternalComposer(Field field)
        {
            this.field = field;
        }

        @Override
        protected Expression compose(Expression argument)
        {
            return new WeekDayNameExpression(argument, field);
        }

        @Override
        protected AkType argumentType(AkType givenType)
        {
            switch(givenType)
            {
                case DATE:
                case TIMESTAMP:
                case DATETIME:  return givenType;
                default:        return AkType.DATETIME;
            }
        }

        @Override
        protected ExpressionType composeType(ExpressionType argumentType)
        {
            switch(field)
            {
                case DAYNAME:   return ExpressionTypes.varchar(9); // max length of a day-of-week name is 9
                default:        return ExpressionTypes.INT;
            }
        }

        @Override
        public String toString()
        {
            return field.name();
        }
    }
    private static class InnerEvaluation extends AbstractUnaryExpressionEvaluation
    {
        private final Field field;
        public InnerEvaluation(ExpressionEvaluation ev, Field field)
        {
            super(ev);
            this.field = field;
        }

        @Override
        public ValueSource eval()
        {
            ValueSource s = operand();
            if (s.isNull()) return NullValueSource.only();
            long l = 0;
            long ymd[] = null;
            switch(s.getConversionType())
            {
                case DATE:      l = s.getDate(); 
                                ymd = Extractors.getLongExtractor(AkType.DATE).getYearMonthDay(l);
                                break;
                case DATETIME:  l = s.getDateTime(); 
                                ymd = Extractors.getLongExtractor(AkType.DATETIME).getYearMonthDay(l);
                                break;
                case TIMESTAMP: l = s.getTimestamp(); break; // number of seconds since 1970
                default:        throw new InvalidArgumentTypeException(s.getConversionType() + " is invalid for dayname()");
            }

            DateTime datetime;
            if (ymd == null) datetime = new DateTime(l * 1000); // timestamp
            else datetime = new DateTime((int)ymd[0],(int)ymd[1],(int)ymd[2],1,1); 

            switch(field)
            {
                case DAYNAME:           return new ValueHolder(AkType.VARCHAR, datetime.dayOfWeek().getAsText());
                
                                        // joda:            mon = 1, ..., sat = 6, sun = 7
                                        // mysql DAYOFWEEK: mon = 2, ..., sat = 7, sun = 1
                case DAYOFWEEK:         return new ValueHolder(AkType.INT, datetime.getDayOfWeek() % 7 +1);

                                        // joda:            mon = 1,..., sat = 6, sun = 7
                                        // mysql WEEKDAY:   mon = 0,..., sat = 5, sun = 6
                default: /*WEEKDAY*/    return new ValueHolder(AkType.INT, datetime.getDayOfWeek() -1);
            }            
        }
    }

    public WeekDayNameExpression (Expression arg, Field field)
    {
        super(field == Field.DAYNAME? AkType.VARCHAR : AkType.INT, arg);
        this.field = field;
    }

    @Override
    protected String name()
    {
        return field.name();
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(operandEvaluation(), field);
    }
    
    private final Field field;
}
