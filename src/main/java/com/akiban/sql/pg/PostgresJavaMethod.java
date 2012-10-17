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

import com.akiban.sql.parser.ParameterNode;
import com.akiban.sql.server.ServerJavaMethod;
import com.akiban.sql.server.ServerJavaRoutine;
import com.akiban.sql.server.ServerRoutineInvocation;

import com.akiban.ais.model.Routine;
import com.akiban.server.types3.Types3Switch;

import java.lang.reflect.Method;
import java.util.List;
import java.io.IOException;

public class PostgresJavaMethod extends PostgresJavaRoutine
{
    private Method method;

    public static PostgresStatement statement(PostgresServerSession server, 
                                              ServerRoutineInvocation invocation,
                                              List<ParameterNode> params, int[] paramTypes) {
        Method method = server.getRoutineLoader().loadJavaMethod(server.getSession(),
                                                                 invocation.getRoutineName());
        Routine routine = invocation.getRoutine();
        List<PostgresType> columnTypes = columnTypes(routine);
        List<String> columnNames;
        if (columnTypes.isEmpty()) {
            columnTypes = null;
            columnNames = null;
        }
        else {
            columnNames = columnNames(routine);
        }
        PostgresType[] parameterTypes;
        if ((params == null) || params.isEmpty())
            parameterTypes = null;
        else
            parameterTypes = parameterTypes(invocation, params.size(), paramTypes);
        boolean usesPValues = server.getBooleanProperty("newtypes", Types3Switch.ON);
        return new PostgresJavaMethod(method, invocation,
                                      columnNames, columnTypes,
                                      parameterTypes, usesPValues);
    }

    protected PostgresJavaMethod(Method method,
                                 ServerRoutineInvocation invocation,
                                 List<String> columnNames, 
                                 List<PostgresType> columnTypes,
                                 PostgresType[] parameterTypes,
                                 boolean usesPValues) {
        super(invocation, columnNames, columnTypes, parameterTypes, usePValues);
        this.method = method;
    }

    @Override
    protected ServerJavaRoutine javaRoutine(PostgresQueryContext context) {
        return new ServerJavaMethod(context, invocation, method);
    }
    
}
