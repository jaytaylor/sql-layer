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

package com.akiban.sql.script;

import com.akiban.ais.model.Parameter;
import com.akiban.ais.model.Routine;
import com.akiban.server.error.ExternalRoutineInvocationException;
import com.akiban.sql.server.ServerJavaRoutine;
import com.akiban.sql.server.ServerJavaValues;
import com.akiban.sql.server.ServerQueryContext;
import com.akiban.sql.server.ServerRoutineInvocation;
import com.akiban.server.service.routines.ScriptInvoker;
import com.akiban.server.service.routines.ScriptPool;

import java.sql.ResultSet;
import java.lang.reflect.Array;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

/** Implementation of the <code>SCRIPT_FUNCTION_JAVA</code> calling convention. 
 * Like standard <code>PARAMETER STYLE JAVA</code>, outputs are passed
 * as 1-long arrays that the called function stores into.
 */
public class ScriptFunctionJavaRoutine extends ServerJavaRoutine
{
    private ScriptPool<ScriptInvoker> pool;
    private Object[] functionArgs;
    private Object functionResult;
    
    public ScriptFunctionJavaRoutine(ServerQueryContext context,
                                     ServerRoutineInvocation invocation,
                                     ScriptPool<ScriptInvoker> pool) {
        super(context, invocation);
        this.pool = pool;
    }

    @Override
    public void push() {
        super.push();
        functionArgs = functionArgs(getInvocation().getRoutine());
    }

    protected static Object[] functionArgs(Routine routine) {
        List<Parameter> parameters = routine.getParameters();
        int dynamicResultSets = routine.getDynamicResultSets();
        Object[] result = new Object[parameters.size() + dynamicResultSets];
        int index = 0;
        for (Parameter parameter : parameters) {
            if (parameter.getDirection() != Parameter.Direction.IN) {
                result[index++] = new Object[1];
            }
        }
        for (int i = 0; i < dynamicResultSets; i++) {
            result[index++] = new Object[1];
        }
        return result;
    }

    @Override
    public void setInParameter(Parameter parameter, ServerJavaValues values, int index) {
        if (parameter.getDirection() == Parameter.Direction.INOUT) {
            Array.set(functionArgs[index], 0, values.getObject(index));
        }
        else {
            functionArgs[index] = values.getObject(index);
        }
    }

    @Override
    public void invoke() {
        ScriptInvoker invoker = pool.get();
        boolean success = false;
        try {
            functionResult = invoker.invoke(functionArgs);
            success = true;
        }
        finally {
            pool.put(invoker, !success);
        }
    }

    @Override
    public Object getOutParameter(Parameter parameter, int index) {
        if (parameter.getDirection() == Parameter.Direction.RETURN) {
            return functionResult;
        }
        else {
            return Array.get(functionArgs[index], 0);
        }
    }
    
    @Override
    public Queue<ResultSet> getDynamicResultSets() {
        Queue<ResultSet> result = new ArrayDeque<ResultSet>();
        for (int index = getInvocation().getRoutine().getParameters().size();
             index < functionArgs.length; index++) {
            ResultSet rs = (ResultSet)((Object[])functionArgs[index])[0];
            if (rs != null)
                result.add(rs);
        }
        return result;
    }

}
