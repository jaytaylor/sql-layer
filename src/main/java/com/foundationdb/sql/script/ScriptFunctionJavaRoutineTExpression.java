/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

package com.foundationdb.sql.script;

import com.foundationdb.ais.model.Routine;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.server.service.routines.ScriptInvoker;
import com.foundationdb.server.service.routines.ScriptPool;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.sql.server.ServerJavaRoutine;
import com.foundationdb.sql.server.ServerJavaRoutineTExpression;
import com.foundationdb.sql.server.ServerQueryContext;
import com.foundationdb.sql.server.ServerRoutineInvocation;

import java.util.List;

public class ScriptFunctionJavaRoutineTExpression extends ServerJavaRoutineTExpression {
    public ScriptFunctionJavaRoutineTExpression(Routine routine,
                                                List<? extends TPreparedExpression> inputs) {
        super(routine, inputs);
    }

    @Override
    protected ServerJavaRoutine javaRoutine(ServerQueryContext context,
                                            QueryBindings bindings,
                                            ServerRoutineInvocation invocation) {
        ScriptPool<ScriptInvoker> pool = context.getServer().getRoutineLoader().
            getScriptInvoker(context.getSession(), routine.getName());
        return new ScriptFunctionJavaRoutine(context, bindings, invocation, pool);
    }

}
