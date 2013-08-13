/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.sql.script;

import com.akiban.ais.model.Parameter;
import com.akiban.qp.operator.QueryBindings;
import com.akiban.sql.server.ServerCallExplainer;
import com.akiban.sql.server.ServerJavaRoutine;
import com.akiban.sql.server.ServerJavaValues;
import com.akiban.sql.server.ServerQueryContext;
import com.akiban.sql.server.ServerRoutineInvocation;
import com.akiban.server.explain.Attributes;
import com.akiban.server.explain.CompoundExplainer;
import com.akiban.server.explain.ExplainContext;
import com.akiban.server.explain.Label;
import com.akiban.server.explain.PrimitiveExplainer;
import com.akiban.server.service.routines.ScriptEvaluator;
import com.akiban.server.service.routines.ScriptPool;

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
        if (bindings.containsKey(var))
            return bindings.get(var);
        // Not bound, try to find in result.
        if (parameter.getRoutine().isProcedure()) {
            // Unless FUNCTION, can usurp return value.
            if (evalResult instanceof Map) {
                return ((Map)evalResult).get(var);
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
        return result;
    }

    @Override
    public void pop(boolean success) {
        pool.put(evaluator, success);
        evaluator = null;
        super.pop(success);
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
