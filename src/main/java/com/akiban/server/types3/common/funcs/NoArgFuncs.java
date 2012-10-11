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

import com.akiban.server.AkServer;
import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TInstanceGenerator;
import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.mcompat.mtypes.MDatetimes;
import com.akiban.server.types3.mcompat.mtypes.MApproximateNumber;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;
import com.akiban.server.types3.texpressions.std.NoArgExpression;
import java.util.Date;

public class NoArgFuncs
{
    static final int USER_NAME_LENGTH = 77;

    public static final TScalar SHORT_SERVER_VERSION = new NoArgExpression("version", true)
    {
        @Override
        public void evaluate(TExecutionContext context, PValueTarget target)
        {
            target.putString(AkServer.SHORT_VERSION_STRING, null);
        }

        @Override
        protected TClass resultTClass() {
            return MString.VARCHAR;
        }

        @Override
        protected int[] resultAttrs() {
            return new int[] { AkServer.SHORT_VERSION_STRING.length() };
        }
    };

    public static final TScalar SERVER_FULL_VERSION = new NoArgExpression("version_full", true)
    {

        @Override
        public void evaluate(TExecutionContext context, PValueTarget target)
        {
            target.putString(AkServer.VERSION_STRING, null);
        }

        @Override
        protected TClass resultTClass() {
            return MString.VARCHAR;
        }

        @Override
        protected int[] resultAttrs() {
            return new int[] { AkServer.VERSION_STRING.length() };
        }
    };
    
    public static final TScalar PI = new TScalarBase()
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
            return TOverloadResult.fixed(MApproximateNumber.DOUBLE);
        }
    };
 
    public static final TScalar CUR_DATE = new NoArgExpression("CURRENT_DATE", true)
    {
        @Override
        public String[] registeredNames()
        {
            return new String[] {"curdate", "current_date"};
        }
        
        @Override
        public TClass resultTClass()
        {
            return MDatetimes.DATE;
        }

        @Override
        public void evaluate(TExecutionContext context, PValueTarget target)
        {
            target.putInt32(MDatetimes.encodeDate(context.getCurrentDate(), context.getCurrentTimezone()));
        }
    };

    public static final TScalar CUR_TIME = new NoArgExpression("CURRENT_TIME", true)
    {
        @Override
        public String[] registeredNames()
        {
            return new String[] {"curtime", "current_time"};
        }
        
        @Override
        public TClass resultTClass()
        {
            return MDatetimes.TIME;
        }

        @Override
        public void evaluate(TExecutionContext context, PValueTarget target)
        {
            target.putInt32(MDatetimes.encodeTime(context.getCurrentDate(), context.getCurrentTimezone()));
        }   
    };

    public static final TScalar CUR_TIMESTAMP = new NoArgExpression("CURRENT_TIMESTAMP", true)
    {
        @Override
        public String[] registeredNames()
        {
            return new String[] {"current_timestamp", "now", "localtime", "localtimestamp"};
        }

        @Override
        public TClass resultTClass()
        {
            return MDatetimes.DATETIME;
        }

        @Override
        public void evaluate(TExecutionContext context, PValueTarget target)
        {
            target.putInt64(MDatetimes.encodeDatetime(context.getCurrentDate(), context.getCurrentTimezone()));
        }
    };
    
    public static final TScalar UNIX_TIMESTAMP = new NoArgExpression("UNIX_TIMESTAMP", true)
    {
        @Override
        public void evaluate(TExecutionContext context, PValueTarget target)
        {
            target.putInt32((int)MDatetimes.encodeTimetamp(context.getCurrentDate(), context));
        }

        @Override
        protected TClass resultTClass()
        {
            return MDatetimes.TIMESTAMP;
        }
        
    };
    
    public static final TScalar SYSDATE = new NoArgExpression("SYSDATE", false)
    {
        @Override
        public TClass resultTClass()
        {
            return MDatetimes.DATETIME;
        }

        @Override
        public void evaluate(TExecutionContext context, PValueTarget target)
        {
            target.putInt64(MDatetimes.encodeDatetime(new Date().getTime(), context.getCurrentTimezone()));
        }
    };

    public static final TScalar CUR_USER;

    static {
        CUR_USER = new NoArgExpression("CURRENT_USER", true) {

            @Override
            public TClass resultTClass() {
                return MString.VARCHAR;
            }

            @Override
            protected int[] resultAttrs() {
                return new int[] { USER_NAME_LENGTH };
            }

            @Override
            public void evaluate(TExecutionContext context, PValueTarget target) {
                target.putString(context.getCurrentUser(), null);
            }
        };
    }

    public static final TScalar SESSION_USER = new NoArgExpression("SESSION_USER", true)
    {
        @Override
        public TClass resultTClass() {
            return MString.VARCHAR;
        }

        @Override
        protected int[] resultAttrs() {
            return new int[] { USER_NAME_LENGTH };
        }

        @Override
        public void evaluate(TExecutionContext context, PValueTarget target)
        {
            target.putString(context.getSessionUser(), null);
        }
    };
    
    public static final TScalar SYSTEM_USER = new NoArgExpression("SYSTEM_USER", true)
    {
        @Override
        public TClass resultTClass() {
            return MString.VARCHAR;
        }

        @Override
        protected int[] resultAttrs() {
            return new int[] { USER_NAME_LENGTH };
        }

        @Override
        public void evaluate(TExecutionContext context, PValueTarget target)
        {
            target.putString(context.getSystemUser(), null);
        }
    };
}
