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
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.mcompat.mtypes.MDateAndTime;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

public abstract class MExtractField extends TScalarBase
{
    private static int MAX_YEAR = 9999;

    public static final TScalar INSTANCES[] = new TScalar[]
    {
        new MExtractField("YEAR", MDateAndTime.DATE, Decoder.DATE)
        {
            @Override
            protected int getField(long[] ymd, TExecutionContext context)
            {
                int ret = (int) ymd[MDateAndTime.YEAR_INDEX];
                return ret > MAX_YEAR ? -1 : ret; // mysql caps the output to [0, 9999]
            }
        },
        new MExtractField("QUARTER", MDateAndTime.DATE, Decoder.DATE)
        {
            @Override
            protected int getField(long[] ymd, TExecutionContext context)
            {
                if (MDateAndTime.isZeroDayMonth(ymd))
                    return 0;

                int month = (int) ymd[MDateAndTime.MONTH_INDEX];

                if (month < 4) return 1;
                else if (month < 7) return 2;
                else if (month < 10) return 3;
                else return 4;
            }
        },
        new MExtractField("MONTH", MDateAndTime.DATE, Decoder.DATE)
        {
            @Override
            protected int getField(long[] ymd, TExecutionContext context)
            {
                return (int) ymd[MDateAndTime.MONTH_INDEX];
            }
        },
        new MExtractField("DAYOFWEEK", MDateAndTime.DATE, Decoder.DATE)
        {
            @Override
            protected int getField(long[] ymd, TExecutionContext context)
            {
                return MDateAndTime.isZeroDayMonth(ymd)
                           ? -1
                           // mysql:  (1 = Sunday, 2 = Monday, …, 7 = Saturday
                           // joda    (7 = Sunday, 1 = mon, l...., 6 = Saturday
                           : MDateAndTime.toJodaDateTime(ymd, context.getCurrentTimezone()).getDayOfWeek()
                             % 7 + 1;

            }
        },
        new MExtractField("WEEKDAY", MDateAndTime.DATE, Decoder.DATE)
        {
            @Override
            protected int getField(long[] ymd, TExecutionContext context)
            {
                return MDateAndTime.isZeroDayMonth(ymd)
                            ? -1
                            //mysql: (0 = Monday, 1 = Tuesday, … 6 = Sunday).
                            //joda:  mon = 1, ..., sat = 6, sun = 7
                           : MDateAndTime.toJodaDateTime(ymd, context.getCurrentTimezone()).getDayOfWeek() - 1;
            }   
        },
        new MExtractField("LAST_DAY", MDateAndTime.DATE, Decoder.DATE)
        {
            @Override
            protected int getField(long[] ymd, TExecutionContext context)
            {
                if (MDateAndTime.isZeroDayMonth(ymd))
                    return -1;
                
                ymd[2] = MDateAndTime.getLastDay(ymd);
                return MDateAndTime.encodeDate(ymd);
            }

            @Override
            public TOverloadResult resultType() {
                return TOverloadResult.fixed(MDateAndTime.DATE);
            }
        },
        new MExtractField("DAYOFYEAR", MDateAndTime.DATE, Decoder.DATE)
        {
            @Override
            protected int getField(long[] ymd, TExecutionContext context)
            {
                return MDateAndTime.isZeroDayMonth(ymd)
                            ? -1
                            : MDateAndTime.toJodaDateTime(ymd, context.getCurrentTimezone()).getDayOfYear();
            }
        },
        new MExtractField("DAY", MDateAndTime.DATE, Decoder.DATE) // day of month
        {   
            @Override
            public String[] registeredNames()
            {
                return new String[]{"DAYOFMONTH", "DAY"};
            }

            @Override
            protected int getField(long[] ymd, TExecutionContext context)
            {
                return (int) ymd[MDateAndTime.DAY_INDEX];
            }
        },
        new MExtractField("HOUR", MDateAndTime.TIME, Decoder.TIME)
        {
            @Override
            protected int getField(long[] ymd, TExecutionContext context)
            {
                // select hour('-10:10:10') should just return 10
                return Math.abs((int) ymd[MDateAndTime.HOUR_INDEX]);
            }
        },
        new MExtractField("MINUTE", MDateAndTime.TIME, Decoder.TIME)
        {
            @Override
            protected int getField(long[] ymd, TExecutionContext context)
            {
                return (int) ymd[MDateAndTime.MIN_INDEX];
            }
        },
        new MExtractField("SECOND", MDateAndTime.TIME, Decoder.TIME)
        {
            @Override
            protected int getField(long[] ymd, TExecutionContext context)
            {
                return (int) ymd[MDateAndTime.SEC_INDEX];
            }
        },
        new TScalarBase() // DAYNAME
        {
            @Override
            protected void buildInputSets(TInputSetBuilder builder)
            {
                builder.covers(MDateAndTime.DATE, 0);
            }

            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
            {
                int date = inputs.get(0).getInt32();
                long ymd[] = MDateAndTime.decodeDate(date);
                if(!MDateAndTime.isValidDate(ymd, MDateAndTime.ZeroFlag.YEAR))
                {
                    output.putNull();
                    context.warnClient(new InvalidDateFormatException("DATE", MDateAndTime.dateToString(date)));
                    return;
                }
                String dayName = MDateAndTime.toJodaDateTime(ymd, context.getCurrentTimezone()).dayOfWeek().
                                                                getAsText(context.getCurrentLocale());
                output.putString(dayName, null);
            }

            @Override
            public String displayName()
            {
                return "DAYNAME";
            }

            @Override
            public TOverloadResult resultType()
            {
                return TOverloadResult.fixed(MString.VARCHAR, 9);
            }
        },
        new TScalarBase() // MONTHNAME
        {
            @Override
            protected void buildInputSets(TInputSetBuilder builder)
            {
                builder.covers(MDateAndTime.DATE, 0);
            }

            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
            {
                int date = inputs.get(0).getInt32();
                long ymd[] = MDateAndTime.decodeDate(date);
                if (!MDateAndTime.isValidDateTime(ymd, MDateAndTime.ZeroFlag.YEAR, MDateAndTime.ZeroFlag.DAY))
                {
                    output.putNull();
                    context.warnClient(new InvalidDateFormatException("DATE", MDateAndTime.dateToString(date)));
                    return;
                }
                
                int numericMonth = (int) MDateAndTime.decodeDate(inputs.get(0).getInt32())[MDateAndTime.MONTH_INDEX];
                String month = MDateAndTime.getMonthName(numericMonth,
                                                         context.getCurrentLocale().getLanguage(),
                                                         context);
                output.putString(month, null);
            }

            @Override
            public String displayName()
            {
                return "MONTHNAME";
            }

            @Override
            public TOverloadResult resultType()
            {
                return TOverloadResult.fixed(MString.VARCHAR, 9);
            }
        }
    };

    protected abstract int getField(long ymd[], TExecutionContext context);

    static enum Decoder
    {
        DATE
        {
            @Override
            long[] decode(long val)
            {
                long ret[] = MDateAndTime.decodeDate(val);
                if (!MDateAndTime.isValidDate_Zeros(ret))
                    return null;
                else
                    return ret;
            }
        },
        DATETIME
        {
            @Override
            long[] decode(long val)
            {
                long ret[] = MDateAndTime.decodeDateTime(val);
                if (!MDateAndTime.isValidDateTime_Zeros(ret))
                    return null;
                else
                    return ret;
            }
        },
        TIME
        {
            @Override
            long[] decode(long val)
            {
                long ret[] = MDateAndTime.decodeTime(val);
                if (!MDateAndTime.isValidHrMinSec(ret, false, false))
                    return null;
                else
                    return ret;
            }
        };
        
        abstract long[] decode(long val);
    }
    private final String name;
    private final TClass inputType;
    private final Decoder decoder;
    private MExtractField (String name, TClass inputType, Decoder decoder)
    {
        this.name = name;
        this.inputType = inputType;
        this.decoder = decoder;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(inputType, 0);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
    {
        int val = inputs.get(0).getInt32();
        long ymd[] = decoder.decode(val);
        int ret;
        if (ymd == null || (ret = getField(ymd, context)) < 0)
        {
            context.warnClient(new InvalidParameterValueException("Invalid DATETIME value: " + val));
            output.putNull();
        }
        else
            output.putInt32(ret);
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
}
