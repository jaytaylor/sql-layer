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

import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TOverload;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.aksql.aktypes.AkString;
import com.akiban.server.types3.mcompat.mtypes.MDatetimes;
import com.akiban.server.types3.mcompat.mtypes.MApproximateNumber;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TOverloadBase;
import com.akiban.server.types3.texpressions.std.NoArgExpression;
import java.util.Date;

public class NoArgFuncs
{
    static final int USER_NAME_LENGTH = 77;

    public static final TOverload PI = new TOverloadBase()
    {
        @Override
        protected void buildInputSets(TInputSetBuilder builder)
        {
            // does nothing. doesn't take any arg
        }

        
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
        {
            output.putDouble(Math.PI);
        }

        @Override
        public String displayName()
        {
            return "PI";
        }

        @Override
        public TOverloadResult resultType()
        {
            return TOverloadResult.fixed(MApproximateNumber.DOUBLE.instance());
        }
    };
 
    public static final TOverload CUR_DATE = new NoArgExpression("CURRENT_DATE", true)
    {
        @Override
        public String[] registeredNames()
        {
            return new String[] {"curdate", "current_date"};
        }
        
        @Override
        public TInstance tInstance()
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
        public String[] registeredNames()
        {
            return new String[] {"curtime", "current_time"};
        }
        
        @Override
        public TInstance tInstance()
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
        public String[] registeredNames()
        {
            return new String[] {"current_timestamp", "now", "localtime", "localtimestamp"};
        }

        @Override
        public TInstance tInstance()
        {
            return MDatetimes.DATETIME.instance();
        }

        @Override
        public void evaluate(TExecutionContext context, PValueTarget target)
        {
            target.putInt64(MDatetimes.encodeDatetime(context.getCurrentDate(), context.getCurrentTimezone()));
        }
    };
    
    public static final TOverload UNIX_TIMESTAMP = new NoArgExpression("UNIX_TIMESTAMP", true)
    {
        @Override
        public void evaluate(TExecutionContext context, PValueTarget target)
        {
            target.putInt32((int)MDatetimes.encodeTimetamp(context.getCurrentDate(), context));
        }

        @Override
        protected TInstance tInstance()
        {
            return MDatetimes.TIMESTAMP.instance();
        }
        
    };
    
    public static final TOverload SYSDATE = new NoArgExpression("SYSDATE", false)
    {
        @Override
        public TInstance tInstance()
        {
            return MDatetimes.DATETIME.instance();
        }

        @Override
        public void evaluate(TExecutionContext context, PValueTarget target)
        {
            target.putInt64(MDatetimes.encodeDatetime(new Date().getTime(), context.getCurrentTimezone()));
        }
    };
    
    public static final TOverload CUR_USER = new NoArgExpression("CURRENT_USER", true)
    {

        @Override
        public TInstance tInstance()
        {
            return AkString.VARCHAR.instance(USER_NAME_LENGTH);
        }

        @Override
        public void evaluate(TExecutionContext context, PValueTarget target)
        {
            target.putString(context.getCurrentUser(), null);
        }
    };
    
    public static final TOverload SESSION_USER = new NoArgExpression("SESSION_USER", true)
    {
        @Override
        public TInstance tInstance()
        {
            return AkString.VARCHAR.instance(USER_NAME_LENGTH);
        }

        @Override
        public void evaluate(TExecutionContext context, PValueTarget target)
        {
            target.putString(context.getSessionUser(), null);
        }
    };
    
    public static final TOverload SYSTEM_USER = new NoArgExpression("SYSTEM_USER", true)
    {
        @Override
        public TInstance tInstance()
        {
            return AkString.VARCHAR.instance(USER_NAME_LENGTH);
        }

        @Override
        public void evaluate(TExecutionContext context, PValueTarget target)
        {
            target.putString(context.getSystemUser(), null);
        }
    };
}
