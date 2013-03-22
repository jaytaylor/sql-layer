
package com.akiban.sql.server;

import com.akiban.ais.model.Routine;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;

import java.lang.reflect.Method;
import java.util.List;

public class ServerJavaMethodExpression extends ServerJavaRoutineExpression {
    public ServerJavaMethodExpression(Routine routine,
                                      List<? extends Expression> inputs) {
        super(routine, inputs);
    }

    @Override
    public ExpressionEvaluation evaluation() {
        return new InnerEvaluation(routine, childrenEvaluations());
    }

    static class InnerEvaluation extends ServerJavaRoutineExpressionEvaluation {
        public InnerEvaluation(Routine routine,
                               List<? extends ExpressionEvaluation> children) {
            super(routine, children);
        }

        @Override
        protected ServerJavaRoutine javaRoutine(ServerQueryContext context,
                                                ServerRoutineInvocation invocation) {
            Method method = context.getServer().getRoutineLoader().
                loadJavaMethod(context.getSession(), routine.getName());
            return new ServerJavaMethod(context, invocation, method);
        }
    }

}
