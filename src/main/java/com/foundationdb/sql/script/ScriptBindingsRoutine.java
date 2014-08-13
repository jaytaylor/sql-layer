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
import com.foundationdb.server.service.routines.ScriptEvaluator;
import com.foundationdb.server.service.routines.ScriptPool;

import javax.script.Bindings;
import java.sql.ResultSet;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.List;
import java.util.Map;

/** Implementation of the <code>SCRIPT_BINDINGS</code> calling convention. 
 * Inputs are passed as named (script engine scope) variables.
 * Outputs can be received in the same way, or (since that is not
 * possible in all languages) via the scripts return value, which can
 * be a single value or a list or a dictionary.
 */
public class ScriptBindingsRoutine extends ServerJavaRoutine
{
    private ScriptPool<ScriptEvaluator> pool;
    private ScriptEvaluator evaluator;
    private Bindings bindings;
    private Object evalResult;

    public ScriptBindingsRoutine(ServerQueryContext context, 
                                 QueryBindings queryBindings,
                                 ServerRoutineInvocation invocation,
                                 ScriptPool<ScriptEvaluator> pool) {
        super(context, queryBindings, invocation);
        this.pool = pool;
    }

    @Override
    public void push() {
        super.push();
        evaluator = pool.get();
        bindings = evaluator.getBindings();
    }

    @Override
    public void setInParameter(Parameter parameter, ServerJavaValues values, int index) {
        String var = parameter.getName();
        if (var == null)
            var = String.format("arg%d", index+1);
        bindings.put(var, values.getObject(index));
    }

    @Override
    public void invoke() {
        evalResult = evaluator.eval(bindings);
    }

    @Override
    public Object getOutParameter(Parameter parameter, int index) {
        if (parameter.getDirection() == Parameter.Direction.RETURN) {
            return evalResult;
        }
        String var = parameter.getName();
        if (var == null)
            var = String.format("arg%d", index+1);
        if (bindings.containsKey(var)) {
            // Rhino Bindings exposes internal objects directly.
            return getRhino17Interface().unwrap(bindings.get(var));
        }
        // Not bound, try to find in result.
        if (parameter.getRoutine().isProcedure()) {
            // Unless FUNCTION, can usurp return value.
            if (evalResult instanceof Map) {
                Map mresult = (Map)evalResult;
                if (mresult.containsKey(var))
                    return mresult.get(var);
                Integer jndex = getParameterArrayPosition(parameter);
                if (mresult.containsKey(jndex))
                    return mresult.get(jndex);
            }
            else if (evalResult instanceof List) {
                List lresult = (List)evalResult;
                int jndex = getParameterArrayPosition(parameter);
                if (jndex < lresult.size())
                    return lresult.get(jndex);
            }
            else {
                for (Parameter otherParam : parameter.getRoutine().getParameters()) {
                    if (otherParam == parameter) continue;
                    if (otherParam.getDirection() != Parameter.Direction.IN)
                        return null; // Too many outputs.
                }
                return evalResult;
            }
        }
        return null;
    }
    
    protected static int getParameterArrayPosition(Parameter parameter) {
        int index = 0;
        for (Parameter otherParam : parameter.getRoutine().getParameters()) {
            if (otherParam == parameter) break;
            if (otherParam.getDirection() != Parameter.Direction.IN)
                index++;
        }
        return index;
    }

    @Override
    public Queue<ResultSet> getDynamicResultSets() {
        Queue<ResultSet> result = new ArrayDeque<>();
        if (evalResult instanceof ResultSet) {
            result.add((ResultSet)evalResult);
        }
        else if (evalResult instanceof List) {
            for (Object obj : (List)evalResult) {
                if (obj instanceof ResultSet) {
                    result.add((ResultSet)obj);
                }
            }
        }
        else if (evalResult instanceof Map) {
            for (Object obj : ((Map)evalResult).values()) {
                if (obj instanceof ResultSet) {
                    result.add((ResultSet)obj);
                }
            }
        }
        return result;
    }

    @Override
    public void pop(boolean success) {
        pool.put(evaluator, success);
        evaluator = null;
        super.pop(success);
    }

    /** In Rhino (1.7), internal Java object wrappers can leak out. 
     * TODO: Needed until completely migrated to Nashorn (Java 8).
     */
    @SuppressWarnings("unchecked")
    static class Rhino17Interface {
        private final Class nativeJavaObject;
        private final java.lang.reflect.Method unwrap;

        public Rhino17Interface() {
            Class clazz = null;
            java.lang.reflect.Method meth = null;
            try {
                clazz = Class.forName("sun.org.mozilla.javascript.NativeJavaObject");
            }
            catch (Exception ex) {
            }
            if (clazz == null) {
                try {
                    clazz = Class.forName("sun.org.mozilla.javascript.internal.NativeJavaObject");
                }
                catch (Exception ex) {
                }
            }
            if (clazz != null) {
                try {
                    meth = clazz.getMethod("unwrap");
                }
                catch (Exception ex) {
                    clazz = null;
                }
            }
            this.nativeJavaObject = clazz;
            this.unwrap = meth;
        }

        public Object unwrap(Object obj) {
            try {
                if ((nativeJavaObject != null) &&
                    nativeJavaObject.isInstance(obj)) {
                    obj = unwrap.invoke(obj);
                }
            }
            catch (Exception ex) {
            }
            return obj;
        }
    }

    private static Rhino17Interface rhino17Interface = null;

    private static Rhino17Interface getRhino17Interface() {
        if (rhino17Interface == null) {
            synchronized (ScriptBindingsRoutine.class) {
                if (rhino17Interface == null) {
                    rhino17Interface = new Rhino17Interface();
                }
            }
        }
        return rhino17Interface;
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context) {
        Attributes atts = new Attributes();
        ScriptEvaluator evaluator = pool.get();
        atts.put(Label.PROCEDURE_IMPLEMENTATION,
                 PrimitiveExplainer.getInstance(evaluator.getEngineName()));
        if (evaluator.isCompiled())
            atts.put(Label.PROCEDURE_IMPLEMENTATION, 
                     PrimitiveExplainer.getInstance("compiled"));
        if (evaluator.isShared())
            atts.put(Label.PROCEDURE_IMPLEMENTATION, 
                     PrimitiveExplainer.getInstance("shared"));
        pool.put(evaluator, true);        
        return new ServerCallExplainer(getInvocation(), atts, context);
    }

}
