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

import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.expression.std.ArithOps.ArithOpComposer;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import java.util.List;

public class DateTimeArithExpression extends ArithExpression
{
    @Scalar({"adddate", "date_add", "addtime"})
    public static final ExpressionComposer ADD_DATE_COMPOSER = new AddSubComposer(ArithOps.ADD);    
    
    @Scalar({"subdate", "date_sub", "subtime"})
    public static final ExpressionComposer SUB_DATE_COMPOSER = new AddSubComposer(ArithOps.MINUS);
    
    @Scalar("timediff")
    public static final ExpressionComposer TIMEDIFF_COMPOSER = new DiffComposer(AkType.TIME)
    {
        @Override
        public void argumentTypes(List<AkType> argumentTypes) 
        {
            if (argumentTypes.size() != 2) throw new WrongExpressionArityException(2, argumentTypes.size());
            
            AkType type;
            for (int n = 0; n < 2; ++n)
                if ((type = argumentTypes.get(n)) != AkType.TIME && type != AkType.TIMESTAMP)
                    argumentTypes.set(n, AkType.TIME);
        }
    };

    @Scalar("datediff")
    public static final ExpressionComposer DATEDIFF_COMPOSER = new DiffComposer(AkType.LONG)
    {
        @Override
        public void argumentTypes(List<AkType> argumentTypes) 
        {
            if (argumentTypes.size() != 2) throw new WrongExpressionArityException(2, argumentTypes.size());
            for (int n = 0; n < 2; ++n)
                argumentTypes.set(n, AkType.DATE);
        }     
    };

    private static class AddSubComposer extends BinaryComposer
    {
        private final ArithOpComposer composer;
        protected AddSubComposer (ArithOpComposer composer)
        {
            this.composer = composer;
        }
        @Override
        protected Expression compose(Expression first, Expression second)
        {
            if (ArithExpression.isNumeric(second.valueType()))
                second = new NumericToIntervalDay(second);
            return composer.compose(first, second);
        }

        @Override
        protected ExpressionType composeType(ExpressionType first, ExpressionType second)
        {
            if (ArithExpression.isNumeric(second.getType()))
                second = ExpressionTypes.INTERVAL_MILLIS;
            return composer.composeType(first, second);
        }

        @Override
        public void argumentTypes(List<AkType> argumentTypes)
        {
            if (argumentTypes.size() != 2) throw new WrongExpressionArityException(2, argumentTypes.size());
            
            // first arg
            AkType firstArg = argumentTypes.get(0);
            AkType secondArg = argumentTypes.get(1);
            if (firstArg != AkType.TIME && firstArg != AkType.TIMESTAMP &&
                    !(firstArg == AkType.DATE && 
                         (secondArg == AkType.INTERVAL_MONTH ||   // to make sure the time portion doesn't
                          ArithExpression.isNumeric(secondArg) && isIntegral(secondArg)))) // get filled  when not needed
                argumentTypes.set(0, AkType.DATETIME);                                          

            // second arg
            // does not need adjusting, since 
            //  - if it's a numeric type, it'll be *casted* to an interval_millis in compose()
            //  - if it's an interval , => expected
            //  - if it's anything else, then InvalidArgumentType will be thrown
        }
        
        protected static boolean isIntegral (AkType type)
        {
            switch (type)
            {
                case DOUBLE:
                case DECIMAL:  return false;
                case LONG:
                case INT:
                case U_BIGINT:  
                default:        return true;
            }
        }
    }

    private abstract static class DiffComposer extends BinaryComposer
    {
        private final AkType topT;
        public DiffComposer (AkType topT)
        {
            this.topT = topT;
        }

        @Override
        protected Expression compose(Expression first, Expression second)
        {
            return new DateTimeArithExpression(first, second, topT);
        }

        @Override
        protected ExpressionType composeType(ExpressionType first, ExpressionType second)
        {
            return ExpressionTypes.newType(topT, 0, 0);
        }
    }

    protected static final class Calculator
    {
        private static final long M_SECS_OF_DAY = 86400000L;

        /**
         *
         * @param interval: in millisecs
         * @return interval in day
         *
         */
        public static long getDay (long interval)
        {
            return interval / M_SECS_OF_DAY;
        }

        /**
         *
         * @param interval: a positive number representing an interval between
         *                  two events in millisecs
         * @return interval in H,M,S
         */
        public static long[] getHMS (long interval)
        {
            long seconds = interval / 1000L;
            long hours = seconds / 3600;
            long minutes = (seconds - hours * 3600) / 60;
            seconds -= (minutes * 60 + hours * 3600);
            return new long[] {hours, minutes, seconds};
        }
    }
    
    protected static class InnerValueSource extends ArithExpression.InnerValueSource
    {
        public InnerValueSource (ArithOp op, AkType topT)
        {
            super(op, topT);
        }

        /**
         *
         * @return number of DAY between two events
         */
        @Override
        public long getLong ()
        {
            return Calculator.getDay(rawInterval());
        }

        /**
         *
         * @return INTERVAL_MILLIS between two events expressed in HOUR:MINUTE:SECOND
         */
        @Override
        public long getTime ()
        {
            check(AkType.TIME);
            long millis = rawInterval();
            long sign;
            if (millis < 0)
                millis *= (sign = -1);
            else
                sign = 1;
            long hms[] = Calculator.getHMS(millis);
            return sign * (hms[0] * 10000L + hms[1] * 100 + hms[2]);
        }
    }

    /**
     *  topT
     *  means INTERVAL_MILLIS expressed in this type
     *  For example, time minus time => interval
     *  if topT is set to TIME => result is INTERVAL in HOURS, SECONDS, MINUTES
     */
    private final AkType topT;
    protected DateTimeArithExpression (Expression left, Expression right, AkType topT)
    {
        super(left, ArithOps.MINUS, right);
        this.topT = topT;
    }

    @Override
    protected InnerValueSource getValueSource (ArithOp op, AkType topT)
    {
        return new InnerValueSource(op, topT);
    }

    @Override
    public ExpressionEvaluation evaluation ()
    {
        return new InnerEvaluation(op, topT, this, childrenEvaluations());
    }
    
}
