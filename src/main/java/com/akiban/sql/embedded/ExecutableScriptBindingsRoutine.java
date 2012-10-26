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

package com.akiban.sql.embedded;

import com.akiban.server.service.routines.ScriptEvaluator;
import com.akiban.server.service.routines.ScriptPool;
import com.akiban.sql.parser.CallStatementNode;
import com.akiban.sql.script.ScriptBindingsRoutine;
import com.akiban.sql.server.ServerRoutineInvocation;
import com.akiban.sql.server.ServerJavaMethod;
import com.akiban.sql.server.ServerJavaRoutine;

class ExecutableScriptBindingsRoutine extends ExecutableJavaRoutine
{
    private ScriptPool<ScriptEvaluator> pool;

    protected ExecutableScriptBindingsRoutine(ScriptPool<ScriptEvaluator> pool,
                                              ServerRoutineInvocation invocation,
                                              JDBCParameterMetaData parameterMetaData) {
        super(invocation, parameterMetaData);
        this.pool = pool;
    }

    public static ExecutableStatement executableStatement(ServerRoutineInvocation invocation,
                                                          JDBCParameterMetaData parameterMetaData,
                                                          CallStatementNode call,
                                                          EmbeddedQueryContext context) {
        JDBCConnection conn = context.getServer();
        ScriptPool<ScriptEvaluator> pool = conn.getRoutineLoader().getScriptEvaluator(conn.getSession(),
                                                                                      invocation.getRoutineName());
        return new ExecutableScriptBindingsRoutine(pool, invocation, parameterMetaData);
    }

    @Override
    protected ServerJavaRoutine javaRoutine(EmbeddedQueryContext context) {
        return new ScriptBindingsRoutine(context, invocation, pool);
    }
    
}
