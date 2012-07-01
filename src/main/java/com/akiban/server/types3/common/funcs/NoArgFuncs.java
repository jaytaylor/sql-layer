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

package com.akiban.server.types3.common.funcs;

import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.Row;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TOverload;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.aksql.aktypes.AkString;
import com.akiban.server.types3.mcompat.mtypes.MDatetimes;
import com.akiban.server.types3.mcompat.mtypes.MApproximateNumber;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TEvaluatableExpression;
import com.akiban.server.types3.texpressions.TPreparedExpression;
import com.akiban.server.types3.texpressions.std.NoArgExpression;
import java.util.Date;

public class NoArgFuncs
{
    static final int MySQL_DEFAULT_LENGTH = 77;

    public static final TPreparedExpression PI = new TPreparedExpression()
    {
        private final PValue VAL = new PValue(Math.PI);
        private final TPreptimeValue PREP_VAL = new TPreptimeValue(MApproximateNumber.DOUBLE.instance(), VAL);
        private final TInstance RESULT_TYPE = MApproximateNumber.DOUBLE.instance();
        
        @Override
        public TPreptimeValue evaluateConstant()
        {
            return PREP_VAL;
        }

        @Override
        public TInstance resultType()
        {
            return RESULT_TYPE;
        }

        @Override
        public TEvaluatableExpression build()
        {
            return new TEvaluatableExpression()
            {
                @Override
                public PValueSource resultValue()
                {
                    return VAL;
                }

                @Override
                public void evaluate()
                {
                    // does nothing
                }

                @Override
                public void with(Row row)
                {
                    // does nothing
                }

                @Override
                public void with(QueryContext context)
                {
                    // does nothing
                }
            };
        }        
    };
 
    public static final TOverload CUR_DATE = new NoArgExpression("CURRENT_DATE", true)
    {
        @Override
        public TInstance tInstance(TExecutionContext context)
        {
            return MDatetimes.DATE.instance();
        }

        @Override
        public void evaluate(TExecutionContext context, PValueTarget target)
        {
            target.putInt32(MDatetimes.encodeDate(context.getCurrentDate(), context.getCurrentTimezone()));
        }
    };

    public static final TOverload CUR_TIME = new NoArgExpression("CURRENT_TIME", true)
    {
        @Override
        public TInstance tInstance(TExecutionContext context)
        {
            return MDatetimes.TIME.instance();
        }

        @Override
        public void evaluate(TExecutionContext context, PValueTarget target)
        {
            target.putInt32(MDatetimes.encodeTime(context.getCurrentDate(), context.getCurrentTimezone()));
        }   
    };

    public static final TOverload CUR_TIMESTAMP = new NoArgExpression("CURRENT_TIMESTAMP", true)
    {
        @Override
        public TInstance tInstance(TExecutionContext context)
        {
            return MDatetimes.DATETIME.instance();
        }

        @Override
        public void evaluate(TExecutionContext context, PValueTarget target)
        {
            target.putInt64(MDatetimes.encodeDatetime(context.getCurrentDate(), context.getCurrentTimezone()));
        }
    };
    
    public static final TOverload SYSDATE = new NoArgExpression("SYSDATE", false)
    {
        @Override
        public TInstance tInstance(TExecutionContext context)
        {
            return MDatetimes.DATETIME.instance();
        }

        @Override
        public void evaluate(TExecutionContext context, PValueTarget target)
        {
            target.putInt64(MDatetimes.encodeDatetime(new Date().getTime(), context.getCurrentTimezone()));
        }
    };
    
    public static final TOverload CUR_USER = new NoArgExpression("CUR_USER", true)
    {

        @Override
        public TInstance tInstance(TExecutionContext context)
        {
            return AkString.VARCHAR.instance(context.getCurrentUser().length());
        }

        @Override
        public void evaluate(TExecutionContext context, PValueTarget target)
        {
            target.putObject(context.getCurrentUser());
        }
    };
    
    public static final TOverload SESSION_USER = new NoArgExpression("SESSION_USER", true)
    {
        @Override
        public TInstance tInstance(TExecutionContext context)
        {
            return AkString.VARCHAR.instance(context.getSessionUser().length());
        }

        @Override
        public void evaluate(TExecutionContext context, PValueTarget target)
        {
            target.putObject(context.getSessionUser());
        }
    };
    
    public static final TOverload SYSTEM_USER = new NoArgExpression("SYSTEM_USER", true)
    {
        @Override
        public TInstance tInstance(TExecutionContext context)
        {
            return AkString.VARCHAR.instance(context.getSystemUser().length());
        }

        @Override
        public void evaluate(TExecutionContext context, PValueTarget target)
        {
            target.putObject(context.getSystemUser());
        }
    };
}
