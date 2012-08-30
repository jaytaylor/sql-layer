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
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TOverload;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.mcompat.mtypes.MDatetimes;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TOverloadBase;
import org.joda.time.MutableDateTime;

public class MDatesub extends TOverloadBase
{
    public static final TOverload[] INSTANCES = new TOverload[]
    {
        //ADDDATE
        new MDatesub(Helper.DO_ADD, FirstType.DATE, SecondType.DAY, "DATE_ADD", "ADDDATE"),
        new MDatesub(Helper.DO_ADD, FirstType.DATETIME, SecondType.DAY, "DATE_ADD", "ADDDATE"),
        new MDatesub(Helper.DO_ADD, FirstType.TIMESTAMP, SecondType.DAY, "DATE_ADD", "ADDDATE"),
        new MDatesub(Helper.DO_ADD, FirstType.TIMESTAMP, SecondType.DAY, "DATE_ADD", "ADDDATE"),
        new MDatesub(Helper.DO_ADD, FirstType.VARCHAR, SecondType.DAY, "DATE_ADD", "ADDDATE")
        {
            protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
            {
                String st
            }
        },
//        new MDatesub(Helper.DO_ADD, FirstType.DATE, SecondType.INTERVAL_MILLIS, "DATE_ADD", "ADDDATE"),
//        new MDatesub(Helper.DO_ADD, FirstType.DATE, SecondType.INTERVAL_MONTH, "DATE_ADD", "ADDDATE"),
//        new MDatesub(Helper.DO_ADD, FirstType.DATETIME, SecondType.INTERVAL_MILLIS, "DATE_ADD", "ADDDATE"),
//        new MDatesub(Helper.DO_ADD, FirstType.DATETIME, SecondType.INTERVAL_MONTH, "DATE_ADD", "ADDDATE"),
//        new MDatesub(Helper.DO_ADD, FirstType.TIMESTAMP, SecondType.INTERVAL_MILLIS, "DATE_ADD", "ADDDATE"),
//        new MDatesub(Helper.DO_ADD, FirstType.TIMESTAMP, SecondType.INTERVAL_MONTH, "DATE_ADD", "ADDDATE"),
        
        // SUBDATE
        new MDatesub(Helper.DO_SUB, FirstType.DATE, SecondType.DAY, "DATE_SUB", "SUBDATE"),
        new MDatesub(Helper.DO_SUB, FirstType.DATETIME, SecondType.DAY, "DATE_SUB", "SUBDATE"),
        new MDatesub(Helper.DO_SUB, FirstType.TIMESTAMP, SecondType.DAY, "DATE_SUB", "SUBDATE"),
//        new MDatesub(Helper.DO_SUB, FirstType.DATE, SecondType.INTERVAL_MILLIS, "DATE_SUB", "SUBDATE"),
//        new MDatesub(Helper.DO_SUB, FirstType.DATE, SecondType.INTERVAL_MONTH, "DATE_SUB", "SUBDATE"),
//        new MDatesub(Helper.DO_SUB, FirstType.DATETIME, SecondType.INTERVAL_MILLIS, "DATE_SUB", "SUBDATE"),
//        new MDatesub(Helper.DO_SUB, FirstType.DATETIME, SecondType.INTERVAL_MONTH, "DATE_SUB", "SUBDATE"),
//        new MDatesub(Helper.DO_SUB, FirstType.TIMESTAMP, SecondType.INTERVAL_MILLIS, "DATE_SUB", "SUBDATE"),
//        new MDatesub(Helper.DO_SUB, FirstType.TIMESTAMP, SecondType.INTERVAL_MONTH, "DATE_SUB", "SUBDATE"),
        
    };

    static class TimeLike extends MDatesub
    {
        TimeLike(Helper h, SecondType sec, String...ns)
        {
            super(h, FirstType.VARCHAR_DATE, sec, ns);
        }

        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
        {
            String st = inputs.get(0).getString();
            
            // 3 'forms' of TIME:
            // (-)hh:mm:ss
            // (-)N hh:mm:ss
            // YYYY-MM-dd hh:mm:ss
            
            int hr = 0, min = 0, sec = 0, offset = 0;;
            String parts[] = st.split("\\s++");
            
            switch(parts.length)
            {
                case 1:
                    final String values[] = st.split(":");
                    try
                    {
                        switch (values.length)
                        {
                            case 3:
                                hr = Integer.parseInt(values[offset++]); // fall
                            case 2:
                                min = Integer.parseInt(values[offset++]); // fall
                            case 1:
                                sec = Integer.parseInt(values[offset]);
                                break;
                            default:
                                context.warnClient(new InvalidDateFormatException("TIME", st));
                                output.putNull();
                                return;
                        }
                    }
                    catch (NumberFormatException ex)
                    {
                        context.warnClient(new InvalidDateFormatException("TIME", st));
                        output.putNull();
                        return;
                    }

                    min += sec / 60;
                    sec %= 60;
                    hr += min / 60;
                    min %= 60;
                    break;
                    
                case 2:
                    break;
                    
                default:
                    context.warnClient(new InvalidDateFormatException("TIME", st));
                    output.putNull();
                    return;
            }
        }
    }
    
    private static enum Helper
    {
        DO_ADD
        {
            protected void compute(MutableDateTime date, long delta)
            {
                date.add(delta);
            }
        },
        DO_SUB
        {
            protected void compute(MutableDateTime date, long delta)
            {
                date.add(-delta);
            }
        };
        
        abstract protected void compute(MutableDateTime date, long delta);
    }

    private static enum FirstType
    {
        VARCHAR_DATE(MString.VARCHAR.instance(29))
        {
            @Override
            long[] decode(PValueSource val, TExecutionContext context)
            {
                String st = val.getString();
                long ret[] = MDatetimes.decodeDate(val.getInt32());
                return  MDatetimes.isValidDayMonth(ret) ? ret : null;
            }
            
            @Override
            protected void putResult(PValueTarget out, MutableDateTime par3, TExecutionContext context)
            {
                
                out.putInt32(MDatetimes.encodeDate(MDatetimes.fromJodaDatetime(par3)));
            }
        },
        DATE(MDatetimes.DATE)
        {
            @Override
            long[] decode(PValueSource val, TExecutionContext context)
            {
                long ret[] = MDatetimes.decodeDate(val.getInt32());
                return  MDatetimes.isValidDayMonth(ret) ? ret : null;
            }
            
            @Override
            protected void putResult(PValueTarget out, MutableDateTime par3, TExecutionContext context)
            {
                out.putInt32(MDatetimes.encodeDate(MDatetimes.fromJodaDatetime(par3)));
            }
            
        },
        TIME(MDatetimes.TIME)
        {
            @Override
            long[] decode(PValueSource val, TExecutionContext context)
            {
                long ret[] = MDatetimes.decodeTime(val.getInt32());
                // TODO use the new method isValidHrMinSec from other branch
                return MDatetimes.isValidHrMinSec(ret) ? ret : null;
            }
            
            @Override
            protected void putResult(PValueTarget out, MutableDateTime par3, TExecutionContext context)
            {
                out.putInt32(MDatetimes.encodeTime(MDatetimes.fromJodaDatetime(par3)));
            }
        },
        DATETIME(MDatetimes.DATETIME)
        {
            @Override
            long[] decode(PValueSource val, TExecutionContext context)
            {
                long ret[] = MDatetimes.decodeDatetime(val.getInt32());
                return !MDatetimes.isValidDatetime(ret) ? ret : null;
            }
            
            @Override
            protected void putResult(PValueTarget out, MutableDateTime par3, TExecutionContext context)
            {
                out.putInt64(MDatetimes.encodeDatetime(MDatetimes.fromJodaDatetime(par3)));
            }
        },
        TIMESTAMP(MDatetimes.TIMESTAMP)
        {
            @Override
            long[] decode(PValueSource val, TExecutionContext context)
            {
                return MDatetimes.decodeTimestamp(val.getInt32(), context.getCurrentTimezone());
            }
            
            @Override
            protected void putResult(PValueTarget out, MutableDateTime par3, TExecutionContext context)
            {
                out.putInt32((int)MDatetimes.encodeTimetamp(par3.getMillis(), context));
            }
        };
        
        FirstType(TClass t)
        {
            type = t.instance();
        }
        
        FirstType(TInstance t)
        {
            type = t;
        }

        abstract long[] decode (PValueSource val, TExecutionContext context);
        protected abstract void putResult(PValueTarget out, MutableDateTime par3, TExecutionContext context);
        
        protected final TInstance type;
    }

    private static enum SecondType
    {
//        INTERVAL_MILLIS(// TODO)
//        {
//            long toMillis(PValueSource arg)
//            {
//                // TODO
//            }
//        },
//        INTERVAL_MONTH(//TODO)
//        {
//            long toMillis(PValueSource arg)
//            {
//                // TODO
//            }
//        },
        DAY(MNumeric.BIGINT)
        {
            @Override
            protected long toMillis(PValueSource arg)
            {
                return arg.getInt64() * MILLS_PER_DAY;
            }
        };

        private SecondType (TClass t)
        {
            type = t;
        }
        
        protected abstract long toMillis(PValueSource arg);
        
        TClass type;
        private static final long MILLS_PER_DAY = 24 * 3600 * 1000;
    }
    
    protected final Helper helper;
    protected final FirstType firstArg;
    protected final SecondType secondArg;
    protected final String names[];

    private MDatesub(Helper h, FirstType first, SecondType sec, String...ns)
    {
        helper = h;
        firstArg = first;
        secondArg = sec;
        names = ns;
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {
        long ymd[] = firstArg.decode(inputs.get(0), context);
        if (ymd == null)
            output.putNull();
        else
        {
            MutableDateTime dt = MDatetimes.toJodaDatetime(ymd, context.getCurrentTimezone());
            helper.compute(dt, secondArg.toMillis(inputs.get(1)));
            firstArg.putResult(output, dt, context);
        }
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(firstArg.type.typeClass(), 0).covers(secondArg.type, 1);
    }

    @Override
    public String displayName()
    {
        return names[0];
    }

    @Override
    public String[] registeredNames()
    {
        return names;
    }
    
    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(firstArg.type);
    }
}
