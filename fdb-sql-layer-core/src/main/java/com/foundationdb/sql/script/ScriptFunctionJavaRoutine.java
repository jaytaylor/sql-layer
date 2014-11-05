/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.sql.script;

import com.foundationdb.ais.model.Parameter;
import com.foundationdb.ais.model.Routine;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.sql.server.ServerCallExplainer;
import com.foundationdb.sql.server.ServerJavaRoutine;
import com.foundationdb.sql.server.ServerJavaValues;
import com.foundationdb.sql.server.ServerQueryContext;
import com.foundationdb.sql.server.ServerRoutineInvocation;
import com.foundationdb.server.explain.Attributes;
import com.foundationdb.server.explain.CompoundExplainer;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.Label;
import com.foundationdb.server.explain.PrimitiveExplainer;
import com.foundationdb.server.service.routines.ScriptInvoker;
import com.foundationdb.server.service.routines.ScriptLibrary;
import com.foundationdb.server.service.routines.ScriptPool;

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
                                     QueryBindings bindings,
                                     ServerRoutineInvocation invocation,
                                     ScriptPool<ScriptInvoker> pool) {
        super(context, bindings, invocation);
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
                result[index] = new Object[1];
            }
            index++;
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
    public void invokeShielded() {
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
        Queue<ResultSet> result = new ArrayDeque<>();
        for (int index = getInvocation().getRoutine().getParameters().size();
             index < functionArgs.length; index++) {
            ResultSet rs = (ResultSet)((Object[])functionArgs[index])[0];
            if (rs != null)
                result.add(rs);
        }
        return result;
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context) {
        Attributes atts = new Attributes();
        ScriptInvoker invoker = pool.get();
        ScriptLibrary library = invoker.getLibrary();
        atts.put(Label.PROCEDURE_IMPLEMENTATION,
                 PrimitiveExplainer.getInstance(library.getEngineName()));
        atts.put(Label.PROCEDURE_IMPLEMENTATION, 
                 PrimitiveExplainer.getInstance(invoker.getFunctionName()));
        if (library.isCompiled())
            atts.put(Label.PROCEDURE_IMPLEMENTATION, 
                     PrimitiveExplainer.getInstance("compiled"));
        pool.put(invoker, true);        
        return new ServerCallExplainer(getInvocation(), atts, context);
    }

}
