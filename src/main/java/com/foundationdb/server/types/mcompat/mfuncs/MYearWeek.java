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

package com.foundationdb.server.types.mcompat.mfuncs;

import com.foundationdb.server.error.InvalidDateFormatException;
import com.foundationdb.server.error.InvalidParameterValueException;
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.mcompat.mtypes.MDateAndTime;
import com.foundationdb.server.types.mcompat.mtypes.MDateAndTime.ZeroFlag;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;
import org.joda.time.DateTimeConstants;
import org.joda.time.DateTimeZone;
import org.joda.time.MutableDateTime;

public abstract class MYearWeek extends TScalarBase
{
    public static final TScalar INSTANCES[] = new TScalar[]
    {
        new MYearWeek("YEARWEEK")
        {
            @Override
            protected int getMode(TExecutionContext context, LazyList<? extends ValueSource> inputs)
            {
                return 0;
            }

            @Override
            protected void buildInputSets(TInputSetBuilder builder)
            {
                builder.covers(MDateAndTime.DATE, 0);
            }
        },
        new MYearWeek("YEARWEEK")
        {
            @Override
            protected int getMode(TExecutionContext context, LazyList<? extends ValueSource> inputs)
            {
                int mode = (int) inputs.get(1).getInt64();
                if (mode < 0 || mode > 7)
                {
                    context.warnClient(new InvalidParameterValueException("MODE out of range [0, 7]: " + mode));
                    return -1;
                }
                return mode;
            }

            @Override
            protected void buildInputSets(TInputSetBuilder builder)
            {
                builder.covers(MDateAndTime.DATE, 0).covers(MNumeric.BIGINT, 1);
            }
        }
    };

    protected abstract int getMode(TExecutionContext context, LazyList<? extends ValueSource> inputs);
    
    private final String name;
    private MYearWeek(String name)
    {
        this.name = name;
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
    {
        int date = inputs.get(0).getInt32();
        long ymd[] = MDateAndTime.decodeDate(date);
        int mode = getMode(context, inputs);
        if (!MDateAndTime.isValidDateTime(ymd, ZeroFlag.YEAR) || mode < 0)
        {
            context.warnClient(new InvalidDateFormatException("Invalid DATE value " , date + ""));
            output.putNull();
        }
        else
        {
            output.putInt32(modes[(int) mode].getYearWeek(new MutableDateTime(DateTimeZone.forID(context.getCurrentTimezone())),
                                                          (int)ymd[0], (int)ymd[1], (int)ymd[2]));
        }
    }

    @Override
    public String displayName()
    {
        return name;
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(MNumeric.INT);
    }
    
    //------------------ static helpers-----------------------------------------
    private static interface Modes
    {
        int getYearWeek(MutableDateTime cal, int yr, int mo, int da);
    }

    private static final Modes[] modes = new Modes[]
    {
      new Modes() {public int getYearWeek(MutableDateTime cal, int yr, int mo, int da){return getMode0257(cal, yr, mo, da, DateTimeConstants.SUNDAY, 0);}}, //0
      new Modes() {public int getYearWeek(MutableDateTime cal, int yr, int mo, int da){return getMode1346(cal, yr, mo, da, DateTimeConstants.SUNDAY,1);}},  //1
      new Modes() {public int getYearWeek(MutableDateTime cal, int yr, int mo, int da){return getMode0257(cal, yr, mo, da, DateTimeConstants.SUNDAY, 0);}}, //2
      new Modes() {public int getYearWeek(MutableDateTime cal, int yr, int mo, int da){return getMode1346(cal, yr, mo, da, DateTimeConstants.SUNDAY, 1);}}, //3
      new Modes() {public int getYearWeek(MutableDateTime cal, int yr, int mo, int da){return getMode1346(cal, yr, mo, da, DateTimeConstants.SATURDAY,4);}},//4
      new Modes() {public int getYearWeek(MutableDateTime cal, int yr, int mo, int da){return getMode0257(cal, yr, mo, da, DateTimeConstants.MONDAY, 5);}}, //5
      new Modes() {public int getYearWeek(MutableDateTime cal, int yr, int mo, int da){return getMode1346(cal, yr, mo, da, DateTimeConstants.SATURDAY,4);}},//6
      new Modes() {public int getYearWeek(MutableDateTime cal, int yr, int mo, int da){return getMode0257(cal, yr, mo, da, DateTimeConstants.MONDAY,5);}},  //7

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
            if (week < 0) return  modes[lowestVal].getYearWeek(cal, yr - 1, 12, 31);
            else return yr * 100 + week / 7 + 1;
        }
        else
        {
            if (week < 0) return yr * 100 + 1;
            else return yr * 100 + week / 7 + 2;
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

        if (dayOfYear < firstD) return modes[lowestVal].getYearWeek(cal, yr - 1, 12, 31);
        else return yr * 100 + (dayOfYear - firstD) / 7 +1;
    }           
}
