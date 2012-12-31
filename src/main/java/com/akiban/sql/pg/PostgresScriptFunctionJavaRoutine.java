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

package com.akiban.sql.pg;

import com.akiban.server.service.routines.ScriptInvoker;
import com.akiban.server.service.routines.ScriptPool;
import com.akiban.sql.script.ScriptFunctionJavaRoutine;
import com.akiban.sql.server.ServerCallInvocation;
import com.akiban.sql.server.ServerJavaMethod;
import com.akiban.sql.server.ServerJavaRoutine;

import java.util.List;
import java.io.IOException;

public class PostgresScriptFunctionJavaRoutine extends PostgresJavaRoutine
{
    private ScriptPool<ScriptInvoker> pool;

    public static PostgresScriptFunctionJavaRoutine statement(PostgresServerSession server, 
                                                              ServerCallInvocation invocation,
                                                              List<String> columnNames, 
                                                              List<PostgresType> columnTypes,
                                                              PostgresType[] parameterTypes,
                                                              boolean usesPValues) {
        ScriptPool<ScriptInvoker> pool = server.getRoutineLoader()
            .getScriptInvoker(server.getSession(), invocation.getRoutineName());
        return new PostgresScriptFunctionJavaRoutine(pool, invocation,
                                                     columnNames, columnTypes,
                                                     parameterTypes, usesPValues);
    }

    protected PostgresScriptFunctionJavaRoutine(ScriptPool<ScriptInvoker> pool,
                                                ServerCallInvocation invocation,
                                                List<String> columnNames, 
                                                List<PostgresType> columnTypes,
                                                PostgresType[] parameterTypes,
                                                boolean usesPValues) {
        super(invocation, columnNames, columnTypes, parameterTypes, usesPValues);
        this.pool = pool;
    }

    @Override
    protected ServerJavaRoutine javaRoutine(PostgresQueryContext context) {
        return new ScriptFunctionJavaRoutine(context, invocation, pool);
    }
    
}
