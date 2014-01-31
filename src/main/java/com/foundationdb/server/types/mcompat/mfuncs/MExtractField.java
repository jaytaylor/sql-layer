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
import com.foundationdb.server.types.mcompat.mtypes.MDatetimes;
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
        new MExtractField("YEAR", MDatetimes.DATE, Decoder.DATE)
        {
            @Override
            protected int getField(long[] ymd, TExecutionContext context)
            {
                int ret = (int) ymd[MDatetimes.YEAR_INDEX];
                return ret > MAX_YEAR ? -1 : ret; // mysql caps the output to [0, 9999]
            }
        },
        new MExtractField("QUARTER", MDatetimes.DATE, Decoder.DATE)
        {
            @Override
            protected int getField(long[] ymd, TExecutionContext context)
            {
                if (MDatetimes.isZeroDayMonth(ymd))
                    return 0;

                int month = (int) ymd[MDatetimes.MONTH_INDEX];

                if (month < 4) return 1;
                else if (month < 7) return 2;
                else if (month < 10) return 3;
                else return 4;
            }
        },
        new MExtractField("MONTH", MDatetimes.DATE, Decoder.DATE)
        {
            @Override
            protected int getField(long[] ymd, TExecutionContext context)
            {
                return (int) ymd[MDatetimes.MONTH_INDEX];
            }
        },
        new MExtractField("DAYOFWEEK", MDatetimes.DATE, Decoder.DATE)
        {
            @Override
            protected int getField(long[] ymd, TExecutionContext context)
            {
                return MDatetimes.isZeroDayMonth(ymd)
                           ? -1
                           // mysql:  (1 = Sunday, 2 = Monday, …, 7 = Saturday
                           // joda    (7 = Sunday, 1 = mon, l...., 6 = Saturday
                           : MDatetimes.toJodaDateTime(ymd, context.getCurrentTimezone()).getDayOfWeek()
                             % 7 + 1;

            }
        },
        new MExtractField("WEEKDAY", MDatetimes.DATE, Decoder.DATE)
        {
            @Override
            protected int getField(long[] ymd, TExecutionContext context)
            {
                return MDatetimes.isZeroDayMonth(ymd)
                            ? -1
                            //mysql: (0 = Monday, 1 = Tuesday, … 6 = Sunday).
                            //joda:  mon = 1, ..., sat = 6, sun = 7
                           : MDatetimes.toJodaDateTime(ymd, context.getCurrentTimezone()).getDayOfWeek() - 1;
            }   
        },
        new MExtractField("LAST_DAY", MDatetimes.DATE, Decoder.DATE)
        {
            @Override
            protected int getField(long[] ymd, TExecutionContext context)
            {
                if (MDatetimes.isZeroDayMonth(ymd))
                    return -1;
                
                ymd[2] = MDatetimes.getLastDay(ymd);
                return MDatetimes.encodeDate(ymd);
            }

            @Override
            public TOverloadResult resultType() {
                return TOverloadResult.fixed(MDatetimes.DATE);
            }
        },
        new MExtractField("DAYOFYEAR", MDatetimes.DATE, Decoder.DATE)
        {
            @Override
            protected int getField(long[] ymd, TExecutionContext context)
            {
                return MDatetimes.isZeroDayMonth(ymd)
                            ? -1
                            : MDatetimes.toJodaDateTime(ymd, context.getCurrentTimezone()).getDayOfYear();
            }
        },
        new MExtractField("DAY", MDatetimes.DATE, Decoder.DATE) // day of month
        {   
            @Override
            public String[] registeredNames()
            {
                return new String[]{"DAYOFMONTH", "DAY"};
            }

            @Override
            protected int getField(long[] ymd, TExecutionContext context)
            {
                return (int) ymd[MDatetimes.DAY_INDEX];
            }
        },
        new MExtractField("HOUR", MDatetimes.TIME, Decoder.TIME)
        {
            @Override
            protected int getField(long[] ymd, TExecutionContext context)
            {
                // select hour('-10:10:10') should just return 10
                return Math.abs((int) ymd[MDatetimes.HOUR_INDEX]);
            }
        },
        new MExtractField("MINUTE", MDatetimes.TIME, Decoder.TIME)
        {
            @Override
            protected int getField(long[] ymd, TExecutionContext context)
            {
                return (int) ymd[MDatetimes.MIN_INDEX];
            }
        },
        new MExtractField("SECOND", MDatetimes.TIME, Decoder.TIME)
        {
            @Override
            protected int getField(long[] ymd, TExecutionContext context)
            {
                return (int) ymd[MDatetimes.SEC_INDEX];
            }
        },
        new TScalarBase() // DAYNAME
        {
            @Override
            protected void buildInputSets(TInputSetBuilder builder)
            {
                builder.covers(MDatetimes.DATE, 0);
            }

            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
            {
                int date = inputs.get(0).getInt32();
                long ymd[] = MDatetimes.decodeDate(date);
                if(!MDatetimes.isValidDate(ymd, MDatetimes.ZeroFlag.YEAR))
                {
                    output.putNull();
                    context.warnClient(new InvalidDateFormatException("DATE", MDatetimes.dateToString(date)));
                    return;
                }
                String dayName = MDatetimes.toJodaDateTime(ymd, context.getCurrentTimezone()).dayOfWeek().
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
                builder.covers(MDatetimes.DATE, 0);
            }

            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
            {
                int date = inputs.get(0).getInt32();
                long ymd[] = MDatetimes.decodeDate(date);
                if (!MDatetimes.isValidDateTime(ymd, MDatetimes.ZeroFlag.YEAR, MDatetimes.ZeroFlag.DAY))
                {
                    output.putNull();
                    context.warnClient(new InvalidDateFormatException("DATE", MDatetimes.dateToString(date)));
                    return;
                }
                
                int numericMonth = (int) MDatetimes.decodeDate(inputs.get(0).getInt32())[MDatetimes.MONTH_INDEX];
                String month = MDatetimes.getMonthName(numericMonth,
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
                long ret[] = MDatetimes.decodeDate(val);
                if (!MDatetimes.isValidDate_Zeros(ret))
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
                long ret[] = MDatetimes.decodeDateTime(val);
                if (!MDatetimes.isValidDateTime_Zeros(ret))
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
                long ret[] = MDatetimes.decodeTime(val);
                if (!MDatetimes.isValidHrMinSec(ret, false, false))
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
