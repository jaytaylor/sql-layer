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

package com.akiban.server.types3;

import com.akiban.server.types3.mcompat.mcasts.CastUtils;
import com.akiban.server.types3.mcompat.mtypes.MDatetimes;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;

public class TParsers
{
    public static final TParser BOOLEAN = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putBool(Boolean.parseBoolean(source.getString()));
        }
    };
    
    public static final TParser TINYINT = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt8((byte) CastUtils.parseInRange(source.getString(),
                                                         Byte.MAX_VALUE, Byte.MIN_VALUE,
                                                         context));
        }
    };
    
        
    public static final TParser UNSIGNED_TINYINT = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt16((short)CastUtils.parseInRange(source.getString(),
                                            Short.MAX_VALUE, Short.MIN_VALUE,
                                            context));
        }
    };
    
    public static final TParser SMALLINT = new TParser()
    {

        @Override
        public void parse(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt16((short)CastUtils.parseInRange(source.getString(),
                                             Short.MAX_VALUE,
                                             Short.MIN_VALUE,
                                             context));
        }
    };

    public static final TParser UNSIGNED_SMALLINT = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt32((int)CastUtils.parseInRange(source.getString(),
                                          Integer.MAX_VALUE, Integer.MIN_VALUE,
                                          context));
        }
    };

    public static final TParser MEDIUMINT = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt32((int)CastUtils.parseInRange(source.getString(),
                                             Integer.MAX_VALUE, Integer.MIN_VALUE,
                                             context));
        }
    };

    public static final TParser UNSIGNED_MEDIUMINT = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt64(CastUtils.parseInRange(source.getString(),
                                     Long.MAX_VALUE, Long.MIN_VALUE,
                                     context));
        }
    };

    public static final TParser INT = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt32((int)CastUtils.parseInRange(source.getString(),
                                           Integer.MAX_VALUE,
                                           Integer.MIN_VALUE,
                                           context));
        }
    };

    public static final TParser UNSIGNED_INT = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt64(CastUtils.parseInRange(source.getString(),
                                     Long.MAX_VALUE, Long.MIN_VALUE,
                                     context));
        }
    };

    public static final TParser BIGINT = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            String st = CastUtils.truncateNonDigits(source.getString(), context);

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

    public static final TParser UNSIGNED_BIGINT = BIGINT; // TODO need a way to handle larger numbers.
    
    public static final TParser FLOAT = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putFloat((float)CastUtils.parseDoubleString(source.getString(), context));
        }
        
    };

    public static final TParser DOUBLE = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putDouble(CastUtils.parseDoubleString(source.getString(), context));
        }
    };
    
    public static final TParser DECIMAL = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            CastUtils.doCastDecimal(context,
                                    CastUtils.parseDecimalString(source.getString(),context),
                                    target);
        }   
    };
    
    public static final TParser DATE = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            int ret = MDatetimes.parseDate(source.getString(), context);
            if (ret < 0)
                target.putNull();
            else
                target.putInt32(ret);
        }
    };

    public static final TParser DATETIME = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt64(MDatetimes.parseDatetime(source.getString()));
        }
    };
    
    public static final TParser TIME = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt32(MDatetimes.parseTime(source.getString(), context));
        }
    };

    public static final TParser TIMESTAMP = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt32(MDatetimes.parseTimestamp(source.getString(), context.getCurrentTimezone(), context));
        }
    };
    
    public static final TParser YEAR = new TParser()
    {
        @Override
        public void parse(TExecutionContext context, PValueSource source, PValueTarget target)
        {
            target.putInt8((byte)CastUtils.parseInRange(source.getString(), Byte.MAX_VALUE, Byte.MIN_VALUE, context));
        }
    };
    
}
