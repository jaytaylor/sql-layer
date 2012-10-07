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

import com.akiban.sql.server.ServerCallContextStack;
import com.akiban.sql.server.ServerJavaValues;
import com.akiban.sql.server.ServerRoutineInvocation;

import com.akiban.ais.model.Routine;
import com.akiban.server.error.ExternalRoutineInvocationException;
import com.akiban.server.types3.Types3Switch;
import com.akiban.util.tap.InOutTap;
import com.akiban.util.tap.Tap;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PostgresJavaMethod extends PostgresDMLStatement
{
    private static final InOutTap EXECUTE_TAP = Tap.createTimer("PostgresLoadableDirectObjectPlan: execute shared");
    private static final InOutTap ACQUIRE_LOCK_TAP = Tap.createTimer("PostgresLoadableDirectObjectPlan: acquire shared lock");

    private Method method;
    private ServerRoutineInvocation invocation;

    public static PostgresStatement statement(PostgresServerSession server, 
                                              ServerRoutineInvocation invocation,
                                              int[] paramTypes) {
        Method method = server.getRoutineLoader().loadJavaMethod(invocation.getRoutineName());
        Routine routine = invocation.getRoutine();
        List<String> columnNames = columnNames(routine);
        List<PostgresType> columnTypes = columnTypes(routine);
        PostgresType[] parameterTypes = parameterTypes(routine, paramTypes);
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
        super(null, columnNames, columnTypes, parameterTypes, usesPValues);
        this.method = method;
        this.invocation = invocation;
    }

    @Override
    protected InOutTap executeTap()
    {
        return EXECUTE_TAP;
    }

    @Override
    protected InOutTap acquireLockTap()
    {
        return ACQUIRE_LOCK_TAP;
    }

    @Override
    public TransactionMode getTransactionMode() {
        return TransactionMode.NONE;
    }

    @Override
    public TransactionAbortedMode getTransactionAbortedMode() {
        return TransactionAbortedMode.NOT_ALLOWED;
    }

    @Override
    public int execute(PostgresQueryContext context, int maxrows) throws IOException {
        PostgresServerSession server = context.getServer();
        PostgresMessenger messenger = server.getMessenger();
        int nrows = 0;
        ServerCallContextStack.push(context, invocation);
        try {
            Object[] methodArgs = methodArgs(method);
            setMethodInputs(method, invocation.asValues(context));
            method.invoke(null, methodArgs);
            if (getColumnTypes() != null) {
                PostgresOutputter<Object[]> outputter = new PostgresJavaMethodResultsOutputter(context, this);
                outputter.output(methodArgs, usesPValues());
                nrows++;
            }
        }
        catch (IllegalAccessException ex) {
            throw new ExternalRoutineInvocationException(invocation.getRoutineName(), ex);
        }
        catch (InvocationTargetException ex) {
            throw new ExternalRoutineInvocationException(invocation.getRoutineName(), ex.getTargetException());
        }
        finally {
            ServerCallContextStack.pop(context, invocation);
        }
        {        
            messenger.beginMessage(PostgresMessages.COMMAND_COMPLETE_TYPE.code());
            messenger.writeString("CALL " + nrows);
            messenger.sendMessage();
        }
        return nrows;
    }

    public Method getMethod() {
        return method;
    }

    public static List<String> columnNames(Routine routine) {
        return null;
    }

    public static List<PostgresType> columnTypes(Routine routine) {
        return null;
    }

    public static PostgresType[] parameterTypes(Routine routine, int[] paramTypes) {
        return null;
    }

    public static Object[] methodArgs(Method method) {
        Class<?>[] parameterTypes = method.getParameterTypes();
        Object[] result = new Object[parameterTypes.length];
        for (int i = 0; i < parameterTypes.length; i++) {
            Class<?> outputType = parameterTypes[i].getComponentType();
            if (outputType != null) {
                result[i] = Array.newInstance(outputType, 1);
            }
        }
        return result;
    }

    public static void setMethodInputs(Method method, ServerJavaValues inputs) {
    }

}
