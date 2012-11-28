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
import com.akiban.sql.server.ServerCallExplainer;
import com.akiban.sql.server.ServerJavaRoutine;
import com.akiban.sql.server.ServerJavaValues;
import com.akiban.sql.server.ServerQueryContext;
import com.akiban.sql.server.ServerRoutineInvocation;
import com.akiban.server.explain.Attributes;
import com.akiban.server.explain.CompoundExplainer;
import com.akiban.server.explain.ExplainContext;
import com.akiban.server.explain.Explainable;
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
                                 ServerRoutineInvocation invocation,
                                 ScriptPool<ScriptEvaluator> pool) {
        super(context, invocation);
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
            else if (getRhino16Interface().isScriptable(evalResult)) {
                return getRhino16Interface().getOutParameter(parameter, var, evalResult);
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
        Queue<ResultSet> result = new ArrayDeque<ResultSet>();
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
        else if (getRhino16Interface().isScriptable(evalResult)) {
            getRhino16Interface().getDynamicResultSets(result, evalResult);
        }
        return result;
    }

    @Override
    public void pop(boolean success) {
        pool.put(evaluator, success);
        evaluator = null;
        super.pop(success);
    }

    /** In Rhino 1.7R3, which comes with JDK 7, Array and Object are List and Map, respectively.
     * In Rhino 1.6R2, which comes with JDK 6, they are not.
     * TODO: Remove when migrated to JDK 7 exclusively.
     */
    static class Rhino16Interface {
        private final Class scriptable, nativeJavaObject;
        private final java.lang.reflect.Method getInt, getString, unwrap;
        private final Object NOT_FOUND;

        public Rhino16Interface() {
            Class c1, c2;
            java.lang.reflect.Method m1, m2, m3;
            Object unique;
            try {
                c1 = Class.forName("sun.org.mozilla.javascript.internal.Scriptable");
                c2 = Class.forName("sun.org.mozilla.javascript.internal.NativeJavaObject");
                unique = c1.getField("NOT_FOUND").get(null);
                m1 = c1.getMethod("get", Integer.TYPE, c1);
                m2 = c1.getMethod("get", String.class, c1);
                m3 = c2.getMethod("unwrap");
            }
            catch (Exception ex) {
                c1 = c2 = null;
                unique = null;
                m1 = m2 = m3 = null;
            }
            this.scriptable = c1;
            this.nativeJavaObject = c2;
            this.getInt = m1;
            this.getString = m2;
            this.unwrap = m3;
            this.NOT_FOUND = unique;
        }

        public boolean isScriptable(Object obj) {
            if (scriptable != null) {
                try {
                    return scriptable.isInstance(obj);
                }
                catch (Exception ex) {
                }
            }
            return false;
        }

        public Object getOutParameter(Parameter parameter, String var, 
                                      Object evalResult) {
            try {
                Object byName = getString.invoke(evalResult, var, null);
                if (byName != NOT_FOUND) return byName;
                int index = getParameterArrayPosition(parameter);
                Object byPosition = getInt.invoke(evalResult, index, null);
                if (byPosition != NOT_FOUND) return byPosition;
            }
            catch (Exception ex) {
            }
            return null;
        }

        public static final int MAX_LENGTH = 100; // Just in case.

        public void getDynamicResultSets(Queue<ResultSet> result, Object evalResult) {
            try {
                for (int i = 0; i < MAX_LENGTH; i++) {
                    Object elem = getInt.invoke(evalResult, i, null);
                    if (elem == NOT_FOUND) break;
                    if (nativeJavaObject.isInstance(elem))
                        elem = unwrap.invoke(elem);
                    if (elem instanceof ResultSet)
                        result.add((ResultSet)elem);
                }
            }
            catch (Exception ex) {
            }
        }
    }

    private static Rhino16Interface rhino16Interface = null;

    private static Rhino16Interface getRhino16Interface() {
        if (rhino16Interface == null) {
            synchronized (ScriptBindingsRoutine.class) {
                if (rhino16Interface == null) {
                    rhino16Interface = new Rhino16Interface();
                }
            }
        }
        return rhino16Interface;
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
