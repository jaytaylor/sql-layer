
package com.akiban.sql.server;

import com.akiban.ais.model.Routine;
import com.akiban.server.types3.texpressions.TPreparedExpression;

import java.lang.reflect.Method;
import java.util.List;

public class ServerJavaMethodTExpression extends ServerJavaRoutineTExpression {
    public ServerJavaMethodTExpression(Routine routine,
                                       List<? extends TPreparedExpression> inputs) {
        super(routine, inputs);
    }

    @Override
    protected ServerJavaRoutine javaRoutine(ServerQueryContext context,
                                            ServerRoutineInvocation invocation) {
        Method method = context.getServer().getRoutineLoader().
            loadJavaMethod(context.getSession(), routine.getName());
        return new ServerJavaMethod(context, invocation, method);
    }

}
