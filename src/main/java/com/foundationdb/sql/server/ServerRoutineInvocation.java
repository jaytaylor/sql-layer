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

package com.foundationdb.sql.server;

import com.foundationdb.ais.model.Parameter;
import com.foundationdb.ais.model.Routine;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.server.types.TInstance;

public abstract class ServerRoutineInvocation
{
    private final Routine routine;

    protected ServerRoutineInvocation(Routine routine) {
        this.routine = routine;
    }

    public int size() {
        return routine.getParameters().size();
    }

    public Routine getRoutine() {
        return routine;
    }

    public Routine.CallingConvention getCallingConvention() {
        return routine.getCallingConvention();
    }

    public TableName getRoutineName() {
        return routine.getName();
    }

    public Parameter getRoutineParameter(int index) {
        if (index == ServerJavaValues.RETURN_VALUE_INDEX)
            return routine.getReturnValue();
        else
            return routine.getParameters().get(index);
    }

    protected TInstance getType(int index) {
        return getRoutineParameter(index).getType();
    }

    public abstract ServerJavaValues asValues(ServerQueryContext queryContext, QueryBindings bindings);
    
}
