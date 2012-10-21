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

package com.akiban.sql.server;

import com.akiban.ais.model.Parameter;

import java.sql.ResultSet;
import java.util.List;

/** A Routine that uses Java native data types in its invocation API. */
public abstract class ServerJavaRoutine
{
    private ServerQueryContext context;
    private ServerRoutineInvocation invocation;

    protected ServerJavaRoutine(ServerQueryContext context,
                                ServerRoutineInvocation invocation) {
        this.context = context;
        this.invocation = invocation;
    }

    public ServerQueryContext getContext() {
        return context;
    }

    public ServerRoutineInvocation getInvocation() {
        return invocation;
    }

    public abstract void setInParameter(Parameter parameter, ServerJavaValues values, int index);
    public abstract void invoke();
    public abstract Object getOutParameter(Parameter parameter, int index);
    public abstract List<ResultSet> getDynamicResultSets();

    public void push() {
        ServerCallContextStack.push(context, invocation);
    }

    public void pop(boolean success) {
        ServerCallContextStack.pop(context, invocation);
    }

    public void setInputs() {
        int nargs = invocation.size();
        ServerJavaValues values = invocation.asValues(context);
        for (int i = 0; i < nargs; i++) {
            Parameter parameter = invocation.getRoutineParameter(i);
            switch (parameter.getDirection()) {
            case IN:
            case INOUT:
                setInParameter(parameter, values, i);
                break;
            }
        }
    }

    public void getOutputs() {
        int nargs = invocation.size();
        ServerJavaValues values = invocation.asValues(context);
        for (int i = 0; i < nargs; i++) {
            Parameter parameter = invocation.getRoutineParameter(i);
            switch (parameter.getDirection()) {
            case INOUT:
            case OUT:
            case RETURN:
                values.setObject(i, getOutParameter(parameter, i));
                break;
            }
        }
    }
}
