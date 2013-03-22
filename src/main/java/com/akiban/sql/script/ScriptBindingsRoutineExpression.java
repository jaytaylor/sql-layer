
package com.akiban.sql.script;

import com.akiban.ais.model.Routine;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.service.routines.ScriptEvaluator;
import com.akiban.server.service.routines.ScriptPool;
import com.akiban.sql.server.ServerJavaRoutine;
import com.akiban.sql.server.ServerJavaRoutineExpression;
import com.akiban.sql.server.ServerJavaRoutineExpressionEvaluation;
import com.akiban.sql.server.ServerQueryContext;
import com.akiban.sql.server.ServerRoutineInvocation;

import java.util.List;

public class ScriptBindingsRoutineExpression extends ServerJavaRoutineExpression {
    public ScriptBindingsRoutineExpression(Routine routine,
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
            ScriptPool<ScriptEvaluator> pool = context.getServer().getRoutineLoader().
                getScriptEvaluator(context.getSession(), routine.getName());
            return new ScriptBindingsRoutine(context, invocation, pool);
        }
    }

}
