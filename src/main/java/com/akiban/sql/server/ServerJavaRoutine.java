
package com.akiban.sql.server;

import java.sql.ResultSet;
import java.util.Queue;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.CacheValueGenerator;
import com.akiban.ais.model.Parameter;
import com.akiban.direct.Direct;
import com.akiban.direct.DirectClassLoader;
import com.akiban.direct.DirectContextImpl;
import com.akiban.server.explain.Explainable;

/** A Routine that uses Java native data types in its invocation API. */
public abstract class ServerJavaRoutine implements Explainable
{
    private static final Object CACHE_KEY = new Object();
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
    public abstract Queue<ResultSet> getDynamicResultSets();

    public void push() {
        ServerCallContextStack.push(context, invocation);
        AkibanInformationSchema ais = context.getServer().getAIS();
        
        DirectClassLoader dcl = ais.getCachedValue(CACHE_KEY, new CacheValueGenerator<DirectClassLoader>() {

            @Override
            public DirectClassLoader valueFor(AkibanInformationSchema ais) {
                return new DirectClassLoader(getClass().getClassLoader(), context.getCurrentSchema(), ais);
            }
            
        });
        Direct.enter(new DirectContextImpl(context.getCurrentSchema(), dcl));
    }

    public void pop(boolean success) {
        Direct.leave();
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
        Parameter parameter = invocation.getRoutineParameter(ServerJavaValues.RETURN_VALUE_INDEX);
        if (parameter != null) {
            values.setObject(ServerJavaValues.RETURN_VALUE_INDEX, getOutParameter(parameter, ServerJavaValues.RETURN_VALUE_INDEX));
        }
    }
}
