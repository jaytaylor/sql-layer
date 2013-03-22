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
