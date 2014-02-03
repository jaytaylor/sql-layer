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
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.mcompat.mtypes.MDateAndTime;
import com.foundationdb.server.types.mcompat.mtypes.MDateAndTime.StringType;
import com.foundationdb.server.types.mcompat.mtypes.MDateAndTime.ZeroFlag;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;
import com.foundationdb.sql.parser.TernaryOperatorNode;

public class MTimestampDiff extends TScalarBase
{
    public static TScalar[] create()
    {
        ArgType args[] = ArgType.values();
        TScalar ret[] = new TScalar[args.length * args.length + 1];
        int n = 0;
        
        for (ArgType arg1 : args)
            for (ArgType arg2 : args)
                ret[n++] = new MTimestampDiff(arg1, arg2)
                {
                    public int[] getPriorities() {return new int[] {0};}
                };
        
        // create a second group prio group, forcing all arguments be
        // casted to the formal types
        ret[n] = new MTimestampDiff(ArgType.TIMESTAMP, ArgType.TIMESTAMP)
        {
            public int[] getPriorities() {return new int[] {1};}
        };
        
        return ret;
    }
    
    private static enum ArgType
    {
        DATE(MDateAndTime.DATE)
        {
            @Override
            long[] getYMD(ValueSource source, TExecutionContext context)
            {
                int date = source.getInt32();
                long ymd[] = MDateAndTime.decodeDate(date);

                if (MDateAndTime.isValidDateTime(ymd, ZeroFlag.YEAR))
                    return ymd;
                else
                {
                    context.warnClient(new InvalidDateFormatException("DATE",
                                                                      MDateAndTime.dateToString(date)));
                    return null;
                }
            }
        },
        DATETIME(MDateAndTime.DATETIME)
        {
            @Override
            long[] getYMD(ValueSource source, TExecutionContext context)
            {
                long datetime = source.getInt64();
                long ymd[] = MDateAndTime.decodeDateTime(datetime);
                
                if (MDateAndTime.isValidDateTime(ymd, ZeroFlag.YEAR))
                    return ymd;
                else
                {
                    context.warnClient(new InvalidDateFormatException("DATETIME",
                                                                      MDateAndTime.dateTimeToString(datetime)));
                    return null;
                }
            }
        },
        TIMESTAMP(MDateAndTime.TIMESTAMP)
        {
            @Override
            long [] getYMD(ValueSource source, TExecutionContext context)
            {
                return MDateAndTime.decodeTimestamp(source.getInt32(), "UTC"/*context.getCurrentTimezone()*/);
            }
            
            // override this because TIMESTAMP type doesn't need to go thru the decoding process
            // just return whatever is passed in
            @Override
            Long getUnix(ValueSource source, TExecutionContext context)
            {
                return source.getInt32() * 1000L; // unix
            }
        },
        VARCHAR(MString.VARCHAR)
        {
            @Override
            long [] getYMD(ValueSource source, TExecutionContext context)
            {
                long ymd[] = new long[6];
                StringType strType = MDateAndTime.parseDateOrTime(source.getString(), ymd);
                if (strType == StringType.TIME_ST || !MDateAndTime.isValidType(strType)) {
                    context.warnClient(new InvalidDateFormatException("DATETIME", source.getString()));
                    return null;
                }
                return ymd;
            }
        }
        ;
        
        abstract long[] getYMD(ValueSource source, TExecutionContext context);
        
        Long getUnix(ValueSource source, TExecutionContext context)
        {
            long ymd[] = getYMD(source, context);

            return ymd == null
                    ? null
                    : MDateAndTime.getTimestamp(ymd, "UTC") * 1000L; // use UTC to do the computation
        }
        
        private ArgType(TClass type)
        {
            this.type = type;
        }
        private final TClass type;
    }
    
    
    private final ArgType arg1;
    private final ArgType arg2;
    
    private MTimestampDiff(ArgType arg1, ArgType arg2)
    {
        this.arg1 = arg1;
        this.arg2 = arg2;
    }
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(MNumeric.INT, 0).covers(arg1.type, 1).covers(arg2.type, 2);
    }
    
    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
    {
        int unit = inputs.get(0).getInt32();
        
        ValueSource date1 = inputs.get(1);
        ValueSource date2 = inputs.get(2);
        
        switch(unit)
        {
            case TernaryOperatorNode.YEAR_INTERVAL:
            case TernaryOperatorNode.QUARTER_INTERVAL:
            case TernaryOperatorNode.MONTH_INTERVAL:
                doMonthSubtraction(arg2.getYMD(date2, context),
                                   arg1.getYMD(date1, context),
                                   MONTH_DIV[unit - MONTH_BASE],
                                   output);
                break;
            case TernaryOperatorNode.WEEK_INTERVAL:
            case TernaryOperatorNode.DAY_INTERVAL:
            case TernaryOperatorNode.HOUR_INTERVAL:
            case TernaryOperatorNode.MINUTE_INTERVAL:
            case TernaryOperatorNode.SECOND_INTERVAL:
            case TernaryOperatorNode.FRAC_SECOND_INTERVAL:
                Long unix1, unix2 = 0L;
                if ((unix1 = arg1.getUnix(date1, context)) == null
                        || (unix2 = arg2.getUnix(date2, context)) == null)
                    output.putNull();
                else
                    output.putInt64((unix2 - unix1) / MILLIS_DIV[unit - MILLIS_BASE]);
                break;
            default:
                        throw new UnsupportedOperationException("Unknown UNIT: " + unit);
        
        }
    }

    @Override
    public String displayName()
    {
        return "TIMESTAMPDIFF";
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(MNumeric.BIGINT, 21);
    }
    
    // ------------ static members --------------
    private static final long[] MILLIS_DIV = new long[6];
    private static final int[] MONTH_DIV = {12, 4, 1};

    private static final int MILLIS_BASE = TernaryOperatorNode.WEEK_INTERVAL;
    private static final int MONTH_BASE = TernaryOperatorNode.YEAR_INTERVAL;
    static
    {
        int mul[] ={7, 24, 60, 60, 1000};

        MILLIS_DIV[5] = 1;
        for (int n = 4; n >= 0; --n)
            MILLIS_DIV[n] = MILLIS_DIV[n + 1] * mul[n];
    }
    
    private static void doMonthSubtraction (long d1[], long d2[], long divisor, ValueTarget out)
    {
        if (d1 == null || d2 == null)
        {
            out.putNull();
            return;
        }
        long ret = (d1[0] - d2[0]) * 12 + d1[1] - d2[1];

        // adjust the day difference
        if (ret > 0 && d1[2] < d2[2]) --ret;
        else if (ret < 0 && d1[2] > d2[2]) ++ret;

        out.putInt64(ret / divisor);
    }

}
