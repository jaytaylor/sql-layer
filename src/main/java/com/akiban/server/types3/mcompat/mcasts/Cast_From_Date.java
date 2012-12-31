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

import com.akiban.server.types3.mcompat.mtypes.MBigDecimalWrapper;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TCastBase;
import com.akiban.server.types3.TCast;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.mcompat.mtypes.MDatetimes;
import com.akiban.server.types3.mcompat.mtypes.MApproximateNumber;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.Constantness;

import static com.akiban.server.types3.mcompat.mtypes.MNumeric.*;

public abstract class Cast_From_Date extends TCastBase
{
    /**
     * TODO:
     * 
     * TIME
     * TIMESTAMP
     * 
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
    public static final TCast TO_TINYINT = new Cast_From_Date(TINYINT)
    {
        @Override
        protected void putOut(int val, PValueTarget out, TExecutionContext context)
        {
            out.putInt8((byte) CastUtils.getInRange(Byte.MAX_VALUE, Byte.MIN_VALUE, val, context));
        }
    };
    
    public static final TCast TO_UNSIGNED_TINYINT = new Cast_From_Date(TINYINT_UNSIGNED)
    {
        @Override
        protected void putOut(int val, PValueTarget out, TExecutionContext context)
        {
            out.putInt16((short)CastUtils.getInRange(Short.MAX_VALUE, 0, val, context));
        }   
    };

    public static final TCast TO_SMALLINT = new Cast_From_Date(SMALLINT)
    {
        @Override
        protected void putOut(int val, PValueTarget out, TExecutionContext context)
        {
            out.putInt16((short)CastUtils.getInRange(Short.MAX_VALUE, Short.MIN_VALUE, val, context));
        }
    };

    public static final TCast TO_UNSIGNED_SMALINT = new Cast_From_Date(SMALLINT_UNSIGNED)
    {
        @Override
        protected void putOut(int val, PValueTarget out, TExecutionContext context)
        {
            out.putInt32((int)CastUtils.getInRange(Integer.MAX_VALUE, 0, val, context));
        }
    };
    
    public static final TCast TO_MEDIUMINT = new Cast_From_Date(MEDIUMINT)
    {
        @Override
        protected void putOut(int val, PValueTarget out, TExecutionContext context)
        {
            out.putInt32(val);
        }
    };

    public static final TCast TO_UNSIGNED_MEDIUMINT = new Cast_From_Date(MEDIUMINT_UNSIGNED)
    {
        @Override
        protected void putOut(int val, PValueTarget out, TExecutionContext context)
        {
            out.putInt64(CastUtils.getInRange(Long.MAX_VALUE, 0, val, context));
        }
    };

    public static final TCast TO_INT = new Cast_From_Date(INT)
    {
        @Override
        protected void putOut(int val, PValueTarget out, TExecutionContext context)
        {
            out.putInt32(val);
        }
    };
    
    public static final TCast TO_UNSIGNED_INT = new Cast_From_Date(INT_UNSIGNED)
    {
        @Override
        protected void putOut(int val, PValueTarget out, TExecutionContext context)
        {
            out.putInt64(CastUtils.getInRange(Long.MAX_VALUE, 0, val, context));
        }
    };
    
    public static final TCast TO_BIGINT = new Cast_From_Date(BIGINT)
    {
        @Override
        protected void putOut(int val, PValueTarget out, TExecutionContext context)
        {
            out.putInt64(val);
        }
    };
    
    public static final TCast TO_UNSIGNED_BIGINT = new Cast_From_Date(BIGINT_UNSIGNED)
    {
        @Override
        protected void putOut(int val, PValueTarget out, TExecutionContext context)
        {
            out.putInt64(val);
        }
    };
    
    public static final TCast TO_DOUBLE = new Cast_From_Date(MApproximateNumber.DOUBLE)
    {
        @Override
        protected void putOut(int val, PValueTarget out, TExecutionContext context)
        {
            out.putDouble(val);
        }
    };
    
    public static final TCast TO_DECIMAL = new Cast_From_Date(MNumeric.DECIMAL)
    {
        @Override
        protected void putOut(int val, PValueTarget out, TExecutionContext context)
        {
            out.putObject(new MBigDecimalWrapper(val));
        }
    };

    public static final TCast TO_DATETIME = new TCastBase(MDatetimes.DATE, MDatetimes.DATETIME, Constantness.UNKNOWN)
    {

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            long[] ymd = MDatetimes.decodeDate(source.getInt32());
            long[] ymdHMS = new long[6];
            System.arraycopy(ymd, 0, ymdHMS, 0, 3);
            long asDate = MDatetimes.encodeDatetime(ymdHMS);
            target.putInt64(asDate);
        }
    };
    
    public static final TCast TO_TIME = new TCastBase(MDatetimes.DATE, MDatetimes.TIME, Constantness.UNKNOWN)
    {

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            // DATE doesn't have any TIME
            target.putInt32(0);
        }
    };
    
    public static final TCast TO_TIMESTAMP = new TCastBase(MDatetimes.DATE, MDatetimes.TIMESTAMP, Constantness.UNKNOWN)
    {

        @Override
        public void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt32(MDatetimes.encodeTimestamp(MDatetimes.decodeDate(source.getInt32()),
                                                       context.getCurrentTimezone(),
                                                       context));
        }
    };
    
    protected abstract void putOut(int val, PValueTarget out, TExecutionContext context);
    
    private Cast_From_Date(TClass targetType)
    {
        super(MDatetimes.DATE, targetType);
    }
    
    @Override
    protected void doEvaluate(TExecutionContext context, PValueSource source, PValueTarget target)
    {
        putOut(packYMD(MDatetimes.decodeDate(source.getInt32())),
               target,
               context);
    }
    
    private static int packYMD(long ymd[])
    {
        return (int) (ymd[MDatetimes.YEAR_INDEX] * 10000
                       + ymd[MDatetimes.MONTH_INDEX] * 100
                       + ymd[MDatetimes.DAY_INDEX]);
    }
}
