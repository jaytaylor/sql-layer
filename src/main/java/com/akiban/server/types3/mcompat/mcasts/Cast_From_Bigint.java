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

package com.akiban.server.types3.mcompat.mcasts;

import com.akiban.server.error.InvalidParameterValueException;
import com.akiban.server.types3.TCast;
import com.akiban.server.types3.TCastBase;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TPreptimeContext;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.mcompat.mtypes.MBigDecimalWrapper;
import com.akiban.server.types3.mcompat.mtypes.MDatetimes;
import com.akiban.server.types3.mcompat.mtypes.MDouble;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.Constantness;
import java.math.BigInteger;

public class Cast_From_Bigint
{
    
    /**
     * TODO:
     * BIT
     * CHAR
     * BINARY
     * VARBINARY
     * TINYBLOG
     * TINYTEXT
     * TEXT
     * MEDIUMBLOB
     * MEDIUMTEXT
     * LONGBLOG
     * LONTTEXT
     * 
     */
    
    public static final TCast TO_TINYINT = new TCastBase(MNumeric.BIGINT, MNumeric.TINYINT, false, Constantness.UNKNOWN)
    {
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt8((byte)getInRange(Byte.MAX_VALUE, Byte.MIN_VALUE, source.getInt64()));
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            return specifiedTarget;
        }
    };
    
    public static final TCast TO_UNSIGNED_TINYINT = new TCastBase(MNumeric.BIGINT, MNumeric.TINYINT_UNSIGNED, false, Constantness.UNKNOWN)
    {
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            // TODO: take the two's complement of the negative value?
            target.putInt8((byte)getInRange(Byte.MAX_VALUE, Byte.MIN_VALUE, source.getInt64()));
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            return specifiedTarget;
        }
    };

    public static final TCast TO_SMALL_INT = new TCastBase(MNumeric.BIGINT, MNumeric.SMALLINT, false, Constantness.UNKNOWN)
    {
        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("not supported yet");
        }

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt16((short)getInRange(Short.MAX_VALUE, Short.MIN_VALUE, source.getInt64()));
        }
    };

    public static final TCast TO_UNSIGNED_SMALL_INT = new TCastBase(MNumeric.BIGINT, MNumeric.SMALLINT_UNSIGNED, false, Constantness.UNKNOWN)
    {

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            // TODO: take the two's complement of signed ==> unsigned
            target.putInt16((short)getInRange(Short.MAX_VALUE, Short.MIN_VALUE, source.getInt64()));
        }
    };
    
    public static final TCast TO_MEDIUM_INT = new TCastBase(MNumeric.BIGINT, MNumeric.MEDIUMINT, false, Constantness.UNKNOWN)
    {
        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt32((int)getInRange(Integer.MAX_VALUE, Integer.MIN_VALUE, source.getInt32()));
        }
        
    };
    
    public static final TCast TO_UNSIGNED_MEDIUM_INT = new TCastBase(MNumeric.BIGINT, MNumeric.MEDIUMINT, false, Constantness.UNKNOWN)
    {
        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            // TODO: signed vs unsigned
            target.putInt32((int)getInRange(Integer.MAX_VALUE, Integer.MIN_VALUE, source.getInt32()));
        }
        
    };
    
    public static final TCast TO_BIGINT = new TCastBase(MNumeric.BIGINT, MNumeric.BIGINT, true, Constantness.UNKNOWN)
    {
        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putValueSource(source);
        }   
    };
    
    public static final TCast TO_UNSIGNED_BIGINT = new TCastBase(MNumeric.BIGINT, MNumeric.BIGINT_UNSIGNED, true, Constantness.UNKNOWN)
    {
        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            // TODO: signed vs unsigned
            target.putValueSource(source);
        }
    };
    
    public static final TCast TO_DECIMAL = new TCastBase(MNumeric.BIGINT, MNumeric.DECIMAL, true, Constantness.UNKNOWN)
    {
        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putObject(new MBigDecimalWrapper(source.getInt64()));
        }
    };
    
    public static final TCast TO_DOUBLE = new TCastBase(MNumeric.BIGINT, MDouble.INSTANCE, true, Constantness.UNKNOWN)
    {
        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putDouble(source.getInt64());
        }   
    };
    
    public static final TCast TO_DATE = new TCastBase(MNumeric.BIGINT, MDatetimes.DATE, true, Constantness.UNKNOWN)
    {
        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            long ymd[] = MDatetimes.fromDate(source.getInt64());
            if (!MDatetimes.isValidDatetime(ymd))
            {
                context.warnClient(new InvalidParameterValueException("Invalid datetime values"));
                target.putNull();
            }
            else
                target.putInt32(MDatetimes.encodeDate(ymd));
        }
    };


    public static final TCast TO_DATETIME = new TCastBase(MNumeric.BIGINT, MDatetimes.DATETIME, true, Constantness.UNKNOWN)
    {
        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            long raw = source.getInt64();
            long ymd[] = MDatetimes.fromDatetime(raw);
                        if (!MDatetimes.isValidDatetime(ymd))
            {
                context.warnClient(new InvalidParameterValueException("Invalid datetime values"));
                target.putNull();
            }
            else
                target.putInt64(raw);
        }
    };
    
    public static final TCast TO_TIMESTAMP = new TCastBase(MNumeric.BIGINT, MDatetimes.TIMESTAMP, true, Constantness.UNKNOWN)
    {
        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            // TIMESTAMPE is underlied by INT32
            target.putInt32((int)getInRange(Integer.MAX_VALUE, Integer.MIN_VALUE, source.getInt64()));
        }
    };
    
    public static final TCast TO_TIME = new TCastBase(MNumeric.BIGINT, MDatetimes.TIME, true, Constantness.UNKNOWN)
    {
        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            long raw = source.getInt64();
            long ymd[] = MDatetimes.fromTime(raw);
                        if (!MDatetimes.isValidDatetime(ymd))
            {
                context.warnClient(new InvalidParameterValueException("Invalid TIME values: " + raw));
                target.putNull();
            }
            else
                target.putInt64(raw);
        }
    };
    
    public static final TCast TO_VARCHAR = new TCastBase(MNumeric.BIGINT, MString.VARCHAR, true, Constantness.UNKNOWN)
    {
        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putObject(Long.toString(source.getInt64()));
        }
    };
    
    private static long getInRange (long max, long min, long val)
    {
        if (val >= max)
            return max;
        else if (val <= min)
            return min;
        else
            return val;
    }

    //TODO: add more
    private static final BigInteger MAX = new BigInteger("18446744073709551615");
}
