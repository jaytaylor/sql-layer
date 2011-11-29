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
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.util.ValueHolder;
import java.util.Calendar;
import java.util.List;

public class WeekExpression extends AbstractCompositeExpression
{
    @Scalar("week")
    public static final ExpressionComposer WEEK_COMPOSER = new InternalComposer();

    private static final class InternalComposer implements ExpressionComposer
    {
        @Override
        public void argumentTypes(List<AkType> argumentTypes)
        {
            int s = argumentTypes.size();
            if (s != 1 && s != 2) throw new WrongExpressionArityException(2, s);
            argumentTypes.set(0, AkType.DATE);
            if (s == 2) argumentTypes.set(1, AkType.INT);            
        }

        @Override
        public ExpressionType composeType(List<? extends ExpressionType> argumentTypes)
        {
            return ExpressionTypes.INT;
        }

        @Override
        public Expression compose(List<? extends Expression> arguments)
        {
            return new WeekExpression(arguments);
        }        
    }

    private static final class InnerEvaluation extends AbstractCompositeExpressionEvaluation
    {
        private static interface Modes
        {
            int getWeek(Calendar cal, int yr, int mo, int da);
        }

        private static final Modes[] modes = new Modes[]
        {
          new Modes() {public int getWeek(Calendar cal, int yr, int mo, int da){return getMode0257(cal, yr, mo, da, Calendar.SUNDAY, 8);}},   // TODO;
          new Modes() {public int getWeek(Calendar cal, int yr, int mo, int da){return getMode1346(cal, yr, mo, da, Calendar.SUNDAY,8);}},   // TODO;
          new Modes() {public int getWeek(Calendar cal, int yr, int mo, int da){return getMode0257(cal, yr, mo, da, Calendar.SUNDAY, 0);}},   // TODO;
          new Modes() {public int getWeek(Calendar cal, int yr, int mo, int da){return getMode1346(cal, yr, mo, da, Calendar.SUNDAY, 1);}},   // TODO;
          new Modes() {public int getWeek(Calendar cal, int yr, int mo, int da){return getMode1346(cal, yr, mo, da, Calendar.SATURDAY,8);}},   // TODO;
          new Modes() {public int getWeek(Calendar cal, int yr, int mo, int da){return getMode0257(cal, yr, mo, da, Calendar.MONDAY, 8);}},   // TODO;
          new Modes() {public int getWeek(Calendar cal, int yr, int mo, int da){return getMode1346(cal, yr, mo, da, Calendar.SATURDAY,4);}},   // TODO;
          new Modes() {public int getWeek(Calendar cal, int yr, int mo, int da){return getMode0257(cal, yr, mo, da, Calendar.MONDAY,5);}},   // TODO;
          new Modes() {public int getWeek(Calendar cal, int yr, int mo, int da){return 0;}} // dummy always return 0-lowestval
        };

        private static int getMode1346(Calendar cal, int yr, int mo, int da, int firstDay, int lowestVal)
        {
            cal.clear();

            // find date of first Sat/Sun:
            cal.set(Calendar.YEAR, yr);
            cal.set(Calendar.MONTH, 0);
            cal.set(Calendar.DAY_OF_MONTH, 1);
            int firstD = 1;

            while (cal.get(Calendar.DAY_OF_WEEK) != firstDay)
                cal.set(Calendar.DAY_OF_MONTH, ++firstD);

            cal.set(Calendar.YEAR, yr);
            cal.set(Calendar.MONTH, mo -1);
            cal.set(Calendar.DAY_OF_MONTH, da);

            int week;

            week = cal.get(Calendar.DAY_OF_YEAR) - (firstD +1 ); // Sun/Mon
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

        private static int getMode0257(Calendar cal, int yr, int mo, int da, int firstDay, int lowestVal)
        {
            cal.clear();

            // find date of first Sun/Mon:
            cal.set(Calendar.YEAR, yr);
            cal.set(Calendar.MONTH, 0);
            cal.set(Calendar.DAY_OF_MONTH, 1);
            int firstD = 1;

            while (cal.get(Calendar.DAY_OF_WEEK) != firstDay)
                cal.set(Calendar.DAY_OF_MONTH, ++firstD);

            cal.clear();
            cal.set(Calendar.YEAR, yr);
            cal.set(Calendar.MONTH, mo -1);
            cal.set(Calendar.DAY_OF_MONTH, da);

            int dayOfYear = cal.get(Calendar.DAY_OF_YEAR);

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

            long date = Extractors.getLongExtractor(AkType.DATE).getLong(fOp);
            int yr = (int)(date / 512);
            int mo = (int)(date / 32 % 16);
            int da = (int)(date % 32);
            long mode = 0;

            // second operand
            if (children().size() == 2)
            {
                ValueSource sOp = children().get(1).eval();
                if (sOp.isNull()) return NullValueSource.only();

                mode = Extractors.getLongExtractor(AkType.INT).getLong(sOp);
            }

            if (mode < 0 || mode > 7) throw new UnsupportedOperationException("MODE of" + mode + " is not valid in WEEK expression");

            return new ValueHolder(AkType.INT, modes[(int)mode].getWeek(Calendar.getInstance(), yr, mo, da));
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
