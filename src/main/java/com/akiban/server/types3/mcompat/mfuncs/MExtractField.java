/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.types3.mcompat.mfuncs;

import com.akiban.server.error.InvalidDateFormatException;
import com.akiban.server.error.InvalidParameterValueException;
import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.mcompat.mtypes.MDatetimes;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;

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
            public String[] registeredNames()
            {
                return new String[]{"MONTH", "MONTHOFYEAR"};
            }
            
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
                           : MDatetimes.toJodaDatetime(ymd, context.getCurrentTimezone()).getDayOfWeek()
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
                           : MDatetimes.toJodaDatetime(ymd, context.getCurrentTimezone()).getDayOfWeek() - 1;
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
                            : MDatetimes.toJodaDatetime(ymd, context.getCurrentTimezone()).getDayOfYear();
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
            protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
            {
                int date = inputs.get(0).getInt32();
                long ymd[] = MDatetimes.decodeDate(date);
                if (!MDatetimes.isValidDayMonth(ymd) || MDatetimes.isZeroDayMonth(ymd))
                {
                    output.putNull();
                    context.warnClient(new InvalidDateFormatException("DATE", date + ""));
                    return;
                }
                String dayName = MDatetimes.toJodaDatetime(ymd,
                                                           context.getCurrentTimezone()).dayOfWeek().
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
            protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
            {
                int date = inputs.get(0).getInt32();
                long ymd[] = MDatetimes.decodeDate(date);
                if (!MDatetimes.isValidDayMonth(ymd) || MDatetimes.isZeroDayMonth(ymd))
                {
                    output.putNull();
                    context.warnClient(new InvalidDateFormatException("DATE", date + ""));
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
                if (!MDatetimes.isValidDayMonth(ret))
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
                long ret[] = MDatetimes.decodeDatetime(val);
                if (!MDatetimes.isValidDatetime(ret))
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
                if (!MDatetimes.isValidHrMinSec(ret, false))
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
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
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
