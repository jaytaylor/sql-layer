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
import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TOverload;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.mcompat.mtypes.MDatetimes;
import com.akiban.server.types3.mcompat.mtypes.MDatetimes.StringType;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TOverloadBase;
import com.akiban.sql.parser.TernaryOperatorNode;

public class MTimestampDiff extends TOverloadBase
{
    public static TOverload[] create()
    {
        ArgType args[] = ArgType.values();
        TOverload ret[] = new TOverload[args.length * args.length];
        int n = 0;
        
        for (ArgType arg1 : args)
            for (ArgType arg2 : args)
                ret[n++] = new MTimestampDiff(arg1, arg2);
        
        return ret;
    }
    
    private static enum ArgType
    {
        DATE(MDatetimes.DATE)
        {
            @Override
            long[] getYMD(PValueSource source, TExecutionContext context)
            {
                int date = source.getInt32();
                long ymd[] = MDatetimes.decodeDate(date);
             
                if (MDatetimes.isValidDatetime(ymd))
                    return ymd;
                else
                {
                    context.warnClient(new InvalidDateFormatException("DATE",
                                                                      MDatetimes.dateToString(date)));
                    return null;
                }
            }
        },
        DATETIME(MDatetimes.DATETIME)
        {
            @Override
            long[] getYMD(PValueSource source, TExecutionContext context)
            {
                long datetime = source.getInt64();
                long ymd[] = MDatetimes.decodeDate(datetime);
                
                if (MDatetimes.isValidDatetime(ymd))
                    return ymd;
                else
                {
                    context.warnClient(new InvalidDateFormatException("DATE",
                                                                      MDatetimes.datetimeToString(datetime)));
                    return null;
                }
            }
        },
        TIMESTAMP(MDatetimes.TIMESTAMP)
        {
            @Override
            long [] getYMD(PValueSource source, TExecutionContext context)
            {
                return MDatetimes.decodeTimestamp(source.getInt32(), "UTC"/*context.getCurrentTimezone()*/);
            }
            
            // override this because TIMESTAMP type doesn't need to go thru the decoding process
            // just return whatever is passed in
            @Override
            Long getUnix(PValueSource source, TExecutionContext context)
            {
                return source.getInt32() * 1000L; // unix
            }
        },
        VARCHAR(MString.VARCHAR)
        {
            @Override
            long [] getYMD(PValueSource source, TExecutionContext context)
            {
                long ymd[] = new long[6];
                InvalidDateFormatException error;
                try
                {
                    StringType strType = MDatetimes.parseDateOrTime(source.getString(), ymd);
                                
                    if (strType == StringType.TIME_ST)
                        error = new InvalidDateFormatException("DATETIME",
                                                               source.getString());
                    else
                        return ymd;
                    
                }
                catch (InvalidDateFormatException e)
                {
                    error = e;
                }

                context.warnClient(error);
                return null;
            }
        }
        ;
        
        abstract long[] getYMD(PValueSource source, TExecutionContext context);
        
        Long getUnix(PValueSource source, TExecutionContext context)
        {
            long ymd[] = getYMD(source, context);

            return ymd == null
                    ? null
                    : MDatetimes.getTimestamp(ymd, "UTC") * 1000L; // use UTC to do the computation
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
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {
        int unit = inputs.get(0).getInt32();
        
        PValueSource date1 = inputs.get(1);
        PValueSource date2 = inputs.get(2);
        
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
        return TOverloadResult.fixed(MNumeric.BIGINT.instance(21));
    }
    
    // -------- static members --------------
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
    
    private static void doMonthSubtraction (long d1[], long d2[], long divisor, PValueTarget out)
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
