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

import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TOverload;
import com.akiban.server.types3.mcompat.mtypes.MDatetimes;
import com.akiban.server.types3.mcompat.mtypes.MDouble;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.std.NoArgExpression;
import java.util.Date;

public class MNoArgFuncs
{
    static final int DEFAULT_LENGTH = 77;

    public static final TOverload PI = new NoArgExpression("PI", true)
    {
        @Override
        public TInstance tInstance(TExecutionContext context)
        {
            return MDouble.INSTANCE.instance();
        }

        @Override
        public void evaluate(TExecutionContext context, PValueTarget target)
        {
            target.putDouble(Math.PI);
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
            target.putInt32(MDatetimes.encodeDate(context.getCurrentDate())); // TODO: define MDatetimes.encodeDate(long millis)
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
            target.putInt32(MDatetimes.encodeTime(context.getCurrentDate())); // TODO:
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
            target.putInt64(MDatetimes.encodeDatetime(context.getCurrentDate()));
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
            target.putInt64(MDatetimes.encodeDatetime(new Date().getTime()));
        }
    };
    
    public static final TOverload CUR_USER = new NoArgExpression("CUR_USER", true)
    {

        @Override
        public TInstance tInstance(TExecutionContext context)
        {
            return MString.VARCHAR.instance(DEFAULT_LENGTH);
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
            return MString.VARCHAR.instance(DEFAULT_LENGTH);
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
            return MString.VARCHAR.instance(DEFAULT_LENGTH);
        }

        @Override
        public void evaluate(TExecutionContext context, PValueTarget target)
        {
            target.putObject(context.getSystemUser());
        }
    };
}
