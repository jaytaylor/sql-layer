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
import com.akiban.server.error.ExternalRoutineInvocationException;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class ServerJavaMethod extends ServerJavaRoutine
{
    private Method method;
    private Class<?>[] parameterTypes;
    private Object[] methodArgs;
    private Object methodResult;
    
    public ServerJavaMethod(ServerQueryContext context,
                            ServerRoutineInvocation invocation,
                            Method method) {
        super(context, invocation);
        this.method = method;
        parameterTypes = method.getParameterTypes();
    }

    @Override
    public void push() {
        super.push();
        methodArgs = methodArgs(parameterTypes);
    }

    protected static Object[] methodArgs(Class<?>[] parameterTypes) {
        Object[] result = new Object[parameterTypes.length];
        for (int i = 0; i < parameterTypes.length; i++) {
            Class<?> outputType = parameterTypes[i].getComponentType();
            if (outputType != null) {
                result[i] = Array.newInstance(outputType, 1);
            }
        }
        return result;
    }

    @Override
    public void setInParameter(Parameter parameter, ServerJavaValues values, int index) {
        Class<?> clazz = parameterTypes[index];
        if (clazz.isArray()) {
            Array.set(methodArgs[index], 0, 
                      values.getObject(index, clazz.getComponentType()));
        }
        else {
            methodArgs[index] = values.getObject(index, clazz);
        }
    }

    @Override
    public void invoke() {
        try {
            methodResult = method.invoke(null, methodArgs);
        }
        catch (IllegalAccessException ex) {
            throw new ExternalRoutineInvocationException(getInvocation().getRoutineName(), ex);
        }
        catch (InvocationTargetException ex) {
            throw new ExternalRoutineInvocationException(getInvocation().getRoutineName(), ex.getTargetException());
        }
    }

    @Override
    public Object getOutParameter(Parameter parameter, int index) {
        if (parameter.getDirection() == Parameter.Direction.RETURN) {
            return methodResult;
        }
        else {
            return Array.get(methodArgs[index], 0);
        }
    }
    
    @Override
    public List<ResultSet> getDynamicResultSets() {
        List<ResultSet> result = new ArrayList<ResultSet>();
        for (int i = 0; i < parameterTypes.length; i++) {
            Class<?> outputType = parameterTypes[i].getComponentType();
            if ((outputType != null) && ResultSet.class.isAssignableFrom(outputType)) {
                ResultSet rs = (ResultSet)Array.get(methodArgs[i], 0);
                if (rs != null)
                    result.add(rs);
            }
        }
        return result;
    }

}
