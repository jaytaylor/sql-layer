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

import com.akiban.server.types3.TCast;
import com.akiban.server.types3.TCastBase;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TPreptimeContext;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.mcompat.mtypes.MDatetimes;
import com.akiban.server.types3.mcompat.mtypes.MDouble;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.Constantness;

/**
 * 
 * This implements a MySQL compatible cast from STRING.
 * ('Compatible' in that the String is parsed as much as possible.)
 * 
 */
public class Cast_From_Varchar
{
    
    /**
     * TODO:
     * 
     * DATETIME
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
      
    public static final TCast TO_TINYINT = new TCastBase(MString.VARCHAR, MNumeric.TINYINT, true, Constantness.UNKNOWN)
    {
        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt8((byte) tryParse((String) source.getObject(),
                                           Byte.MAX_VALUE, Byte.MIN_VALUE,
                                           context));
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    };
    public static final TCast TO_UNSIGNED_TINYINT = new TCastBase(MString.VARCHAR, MNumeric.TINYINT_UNSIGNED, true, Constantness.UNKNOWN)
    {
        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt16((short)tryParse((String)source.getObject(),
                                            Short.MAX_VALUE, Short.MIN_VALUE,
                                            context));
        }
    };
    public static final TCast TO_SMALLINT = new TCastBase(MString.VARCHAR, MNumeric.SMALLINT, true, Constantness.UNKNOWN)
    {
        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt16((short) tryParse((String) source.getObject(),
                                             Short.MAX_VALUE,
                                             Short.MIN_VALUE,
                                             context));
        }
    };
    public static final TCast TO_UNSIGNED_SMALLINT = new TCastBase(MString.VARCHAR, MNumeric.SMALLINT_UNSIGNED, true, Constantness.UNKNOWN)
    {
        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt32((int)tryParse((String)source.getObject(),
                                          Integer.MAX_VALUE, Integer.MIN_VALUE,
                                          context));
        }
    };
    public static final TCast TO_MEDIUMINT = new TCastBase(MString.VARCHAR, MNumeric.MEDIUMINT, true, Constantness.UNKNOWN)
    {
        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt32((int) tryParse((String) source.getObject(),
                                             Integer.MAX_VALUE, Integer.MIN_VALUE,
                                             context));
        }
    };
    public static final TCast TO_UNSIGNED_MEDIUMINT = new TCastBase(MString.VARCHAR, MNumeric.MEDIUMINT_UNSIGNED, true, Constantness.UNKNOWN)
    {
        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt64(tryParse((String)source.getObject(),
                                     Long.MAX_VALUE, Long.MIN_VALUE,
                                     context));
        }
    };
    public static final TCast TO_INT = new TCastBase(MString.VARCHAR, MNumeric.INT, true, Constantness.UNKNOWN)
    {
        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt32((int) tryParse((String) source.getObject(),
                                           Integer.MAX_VALUE,
                                           Integer.MIN_VALUE,
                                           context));
        }
    };
    public static final TCast TO_UNSIGNED_INT = new TCastBase(MString.VARCHAR, MNumeric.INT_UNSIGNED, true, Constantness.UNKNOWN)
    {
        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt64(tryParse((String)source.getObject(),
                                     Long.MAX_VALUE, Long.MIN_VALUE,
                                     context));
        }
    };
    public static final TCast TO_BIGINT = new TCastBase(MString.VARCHAR, MNumeric.BIGINT, true, Constantness.UNKNOWN)
    {
        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            String st = CastUtils.truncateNonDigits((String) source.getObject(), context);

            try
            {
                target.putInt64(Long.parseLong(st));
            }
            catch (NumberFormatException e) // overflow error
            {
                context.reportOverflow(e.getMessage());
                target.putInt64(Long.MAX_VALUE);
            }
        }
    };
    public static final TCast TO_UNSIGNED_BIGINT = new TCastBase(MString.VARCHAR, MNumeric.BIGINT_UNSIGNED, true, Constantness.UNKNOWN)
    {
        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            // BigInteger ?
            throw new UnsupportedOperationException("not supported yet");
        }
    };
    public static final TCast TO_DOUBLE = new TCastBase(MString.VARCHAR, MDouble.INSTANCE, true, Constantness.UNKNOWN)
    {
        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putDouble(CastUtils.parseDoubleString((String)source.getObject(), context));
        }
    };
    public static final TCast TO_DATE = new TCastBase(MString.VARCHAR, MDatetimes.DATE, true, Constantness.UNKNOWN)
    {
        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            int ret = MDatetimes.parseDate((String) source.getObject(), context);
            if (ret < 0)
                target.putNull();
            else
                target.putInt32(ret);
        }
    };

    private static long tryParse(String st, long max, long min, TExecutionContext context)
    {
        String truncated;

        // first attempt
        try
        {
            return CastUtils.getInRange(Long.parseLong(st), max, min, context);
        }
        catch (NumberFormatException e)
        {
            truncated = CastUtils.truncateNonDigits(st, context);
        }

        // second attempt
        return CastUtils.getInRange(Long.parseLong(truncated), max, min, context);
    }
    
    // TODO: add more
  
}
