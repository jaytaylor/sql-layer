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
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.akiban.server.types3.aksql.akfuncs;

import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TOverload;
import com.akiban.server.types3.aksql.aktypes.AkNumeric;
import com.akiban.server.types3.aksql.aktypes.AkString;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.std.NoArgExpression;

public class AkNoArgFuncs
{
    public static final TOverload PI = new NoArgExpression("PI", true)
    {
        @Override
        public TInstance tInstance(TExecutionContext context)
        {
            return AkNumeric.DOUBLE.instance();
        }

        @Override
        public void evaluate(TExecutionContext context, PValueTarget target)
        {
            target.putDouble(Math.PI);
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
