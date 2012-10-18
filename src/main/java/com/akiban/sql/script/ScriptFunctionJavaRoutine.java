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
import com.akiban.server.error.ExternalRoutineInvocationException;
import com.akiban.sql.server.ServerJavaRoutine;
import com.akiban.sql.server.ServerJavaValues;
import com.akiban.sql.server.ServerQueryContext;
import com.akiban.sql.server.ServerRoutineInvocation;
import com.akiban.server.service.routines.ScriptInvoker;
import com.akiban.server.service.routines.ScriptPool;

import java.lang.reflect.Array;
import java.util.List;

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
        functionArgs = functionArgs(getInvocation().getRoutine().getParameters());
    }

    protected static Object[] functionArgs(List<Parameter> parameters) {
        Object[] result = new Object[parameters.size()];
        for (int i = 0; i < result.length; i++) {
            if (parameters.get(i).getDirection() != Parameter.Direction.IN) {
                result = new Object[1];
            }
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
    
}
