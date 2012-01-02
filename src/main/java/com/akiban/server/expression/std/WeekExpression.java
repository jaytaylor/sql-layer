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

import com.akiban.server.error.InvalidParameterValueException;
import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.Extractors;
import com.akiban.sql.StandardException;
import com.akiban.sql.optimizer.ArgList;
import java.util.Arrays;
import java.util.List;
import org.joda.time.MutableDateTime;

public class WeekExpression extends AbstractCompositeExpression
{
    @Scalar("week")
    public static final ExpressionComposer WEEK_COMPOSER = new InternalComposer();

    @Scalar("weekofyear")
    public static final ExpressionComposer WEEK_OF_YEAR_COMPOSER = new UnaryComposer ()
    {
        @Override
        protected Expression compose(Expression argument)
        {
            return new WeekExpression(Arrays.asList(argument, new LiteralExpression(AkType.INT, 3L)));
        }

        @Override
        public ExpressionType composeType(ArgList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 1)
                throw new WrongExpressionArityException(1, argumentTypes.size());
           InternalComposer.adjustFirstArg(argumentTypes);
           return ExpressionTypes.INT;
        }
    };
    
    private static final class InternalComposer implements ExpressionComposer
    {
        @Override
        public Expression compose(List<? extends Expression> arguments)
        {
            return new WeekExpression(arguments);
        }

        @Override
        public ExpressionType composeType(ArgList argumentTypes) throws StandardException
        {
            switch(argumentTypes.size())
            {
                case 2: argumentTypes.setArgType(1, AkType.INT); // fall thru
                case 1: InternalComposer.adjustFirstArg(argumentTypes);
                        return ExpressionTypes.INT;
                default: throw new WrongExpressionArityException(2, argumentTypes.size());
            }
        }

        protected static void adjustFirstArg (ArgList argumentTypes) throws StandardException
        {
            ExpressionType arg = argumentTypes.get(0);
            switch(arg.getType())
            {
                case DATE:
                case DATETIME:
                case TIMESTAMP:  break;
                case VARCHAR:    argumentTypes.setArgType(0, arg.getPrecision() > 10?
                                                             AkType.DATETIME : AkType.DATE);
                                 break;
                default:         argumentTypes.setArgType(0, AkType.DATE);

            }
        }
    }

    private static final class InnerEvaluation extends AbstractCompositeExpressionEvaluation
    {
        private static final class DayOfWeek
        {
            public static final int MON = 1;
            public static final int TUE = 2;
            public static final int WED = 3;
            public static final int THU = 4;
            public static final int FRI = 5;
            public static final int SAT = 6;
            public static final int SUN = 7;
        }
        private static interface Modes
        {
            int getWeek(MutableDateTime cal, int yr, int mo, int da);
        }

        private static final Modes[] modes = new Modes[]
        {
          new Modes() {public int getWeek(MutableDateTime cal, int yr, int mo, int da){return getMode0257(cal, yr, mo, da, DayOfWeek.SUN, 8);}}, //0
          new Modes() {public int getWeek(MutableDateTime cal, int yr, int mo, int da){return getMode1346(cal, yr, mo, da, DayOfWeek.SUN,8);}},  //1
          new Modes() {public int getWeek(MutableDateTime cal, int yr, int mo, int da){return getMode0257(cal, yr, mo, da, DayOfWeek.SUN, 0);}}, //2
          new Modes() {public int getWeek(MutableDateTime cal, int yr, int mo, int da){return getMode1346(cal, yr, mo, da, DayOfWeek.SUN, 1);}}, //3
          new Modes() {public int getWeek(MutableDateTime cal, int yr, int mo, int da){return getMode1346(cal, yr, mo, da, DayOfWeek.SAT,8);}},//4
          new Modes() {public int getWeek(MutableDateTime cal, int yr, int mo, int da){return getMode0257(cal, yr, mo, da, DayOfWeek.MON, 8);}}, //5
          new Modes() {public int getWeek(MutableDateTime cal, int yr, int mo, int da){return getMode1346(cal, yr, mo, da, DayOfWeek.SAT,4);}},//6
          new Modes() {public int getWeek(MutableDateTime cal, int yr, int mo, int da){return getMode0257(cal, yr, mo, da, DayOfWeek.MON,5);}},  //7
          new Modes() {public int getWeek(MutableDateTime cal, int yr, int mo, int da){return 0;}} // dummy always return 0-lowestval
        };

        private static int getMode1346(MutableDateTime cal, int yr, int mo, int da, int firstDay, int lowestVal)
        {
            cal.setYear(yr);
            cal.setMonthOfYear(1);
            cal.setDayOfMonth(1);

            int firstD = 1;

            while (cal.getDayOfWeek() != firstDay) 
                cal.setDayOfMonth(++firstD);

            cal.setYear(yr);
            cal.setMonthOfYear(mo);
            cal.setDayOfMonth(da);

            int week = cal.getDayOfYear() - (firstD +1 ); // Sun/Mon
            if (firstD < 4)
            {
                if (week < 0) return modes[lowestVal].getWeek(cal, yr-1, 12, 31);
                else return week / 7 + 1;
            }
            else
            {
                if (week < 0) return 1;
                else return week / 7 + 2;
            }
        }

        private static int getMode0257(MutableDateTime cal, int yr, int mo, int da, int firstDay, int lowestVal)
        {
            cal.setYear(yr);
            cal.setMonthOfYear(1);
            cal.setDayOfMonth(1);
            int firstD = 1;

            while (cal.getDayOfWeek() != firstDay)
                cal.setDayOfMonth(++firstD);

            cal.setYear(yr); 
            cal.setMonthOfYear(mo); 
            cal.setDayOfMonth(da); 

            int dayOfYear = cal.getDayOfYear(); 

            if (dayOfYear < firstD) return modes[lowestVal].getWeek(cal, yr-1, 12, 31);
            else return (dayOfYear - firstD) / 7 +1;
        }

        public InnerEvaluation (List<? extends ExpressionEvaluation> evals)
        {
            super(evals);
        }

        @Override
        public ValueSource eval()
        {
            // first operand
            ValueSource fOp = children().get(0).eval();
            if (fOp.isNull()) return NullValueSource.only();

            long rawLong = Extractors.getLongExtractor(fOp.getConversionType()).getLong(fOp);
            long ymd[] = Extractors.getLongExtractor(fOp.getConversionType()).getYearMonthDayHourMinuteSecond(rawLong);
            if (ymd[0] * ymd[1] * ymd[2] == 0) throw new InvalidParameterValueException();

            // second operand
            int mode = 0;
            if (children().size() == 2)
            {
                ValueSource sOp = children().get(1).eval();
                if (sOp.isNull()) return NullValueSource.only();

                mode = (int)sOp.getInt();
            }
            
            if (mode < 0 || mode > 7)
                throw new InvalidParameterValueException();
            else
            {
                valueHolder().putRaw(AkType.INT, modes[(int) mode].getWeek(new MutableDateTime(), (int)ymd[0], (int)ymd[1], (int)ymd[2]));
                return valueHolder();
            }
        }
    }

    protected WeekExpression (List<? extends Expression> children)
    {
        super(AkType.INT, checkArgs(children) );
    }
    
    protected static List<? extends Expression> checkArgs (List<? extends Expression> c)
    {
        if (c.size() != 1 && c.size() != 2) throw new WrongExpressionArityException(2, c.size());
        else return c;
    }
    @Override
    protected boolean nullIsContaminating()
    {
        return true;
    }

    @Override
    protected void describe(StringBuilder sb)
    {
        sb.append("WEEK(date,[mode])");
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(childrenEvaluations());
    }
}
