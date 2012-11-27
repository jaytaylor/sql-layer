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

import com.akiban.ais.model.Routine;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.service.routines.ScriptInvoker;
import com.akiban.server.service.routines.ScriptPool;
import com.akiban.sql.server.ServerJavaRoutine;
import com.akiban.sql.server.ServerJavaRoutineExpression;
import com.akiban.sql.server.ServerJavaRoutineExpressionEvaluation;
import com.akiban.sql.server.ServerQueryContext;
import com.akiban.sql.server.ServerRoutineInvocation;

import java.util.List;

public class ScriptFunctionJavaRoutineExpression extends ServerJavaRoutineExpression {
    public ScriptFunctionJavaRoutineExpression(Routine routine,
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
            ScriptPool<ScriptInvoker> pool = context.getServer().getRoutineLoader().
                getScriptInvoker(context.getSession(), routine.getName());
            return new ScriptFunctionJavaRoutine(context, invocation, pool);
        }
    }

}
