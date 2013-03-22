
package com.akiban.sql.script;

import com.akiban.ais.model.Routine;
import com.akiban.server.service.routines.ScriptInvoker;
import com.akiban.server.service.routines.ScriptPool;
import com.akiban.server.types3.texpressions.TPreparedExpression;
import com.akiban.sql.server.ServerJavaRoutine;
import com.akiban.sql.server.ServerJavaRoutineTExpression;
import com.akiban.sql.server.ServerQueryContext;
import com.akiban.sql.server.ServerRoutineInvocation;

import java.util.List;

public class ScriptFunctionJavaRoutineTExpression extends ServerJavaRoutineTExpression {
    public ScriptFunctionJavaRoutineTExpression(Routine routine,
                                                List<? extends TPreparedExpression> inputs) {
        super(routine, inputs);
    }

    @Override
    protected ServerJavaRoutine javaRoutine(ServerQueryContext context,
                                            ServerRoutineInvocation invocation) {
        ScriptPool<ScriptInvoker> pool = context.getServer().getRoutineLoader().
            getScriptInvoker(context.getSession(), routine.getName());
        return new ScriptFunctionJavaRoutine(context, invocation, pool);
    }

}
