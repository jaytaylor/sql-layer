
package com.akiban.sql.script;

import com.akiban.ais.model.Routine;
import com.akiban.server.service.routines.ScriptEvaluator;
import com.akiban.server.service.routines.ScriptPool;
import com.akiban.server.types3.texpressions.TPreparedExpression;
import com.akiban.sql.server.ServerJavaRoutine;
import com.akiban.sql.server.ServerJavaRoutineTExpression;
import com.akiban.sql.server.ServerQueryContext;
import com.akiban.sql.server.ServerRoutineInvocation;

import java.util.List;

public class ScriptBindingsRoutineTExpression extends ServerJavaRoutineTExpression {
    public ScriptBindingsRoutineTExpression(Routine routine,
                                            List<? extends TPreparedExpression> inputs) {
        super(routine, inputs);
    }

    @Override
    protected ServerJavaRoutine javaRoutine(ServerQueryContext context,
                                            ServerRoutineInvocation invocation) {
        ScriptPool<ScriptEvaluator> pool = context.getServer().getRoutineLoader().
            getScriptEvaluator(context.getSession(), routine.getName());
        return new ScriptBindingsRoutine(context, invocation, pool);
    }

}
