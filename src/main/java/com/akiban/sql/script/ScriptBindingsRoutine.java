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
import com.akiban.server.service.routines.ScriptEvaluator;
import com.akiban.server.service.routines.ScriptPool;

import javax.script.Bindings;
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
        bindings = evaluator.createBindings();
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
                int jndex = 0;
                for (Parameter otherParam : parameter.getRoutine().getParameters()) {
                    if (otherParam == parameter) break;
                    if (otherParam.getDirection() != Parameter.Direction.IN)
                        jndex++;
                }
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
    
    @Override
    public void pop() {
        pool.put(evaluator, false);
        evaluator = null;
        super.pop();
    }

}
