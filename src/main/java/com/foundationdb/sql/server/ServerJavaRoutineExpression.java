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

package com.foundationdb.sql.server;

import com.foundationdb.ais.model.Routine;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.expression.std.AbstractCompositeExpression;

import java.util.List;

public abstract class ServerJavaRoutineExpression extends AbstractCompositeExpression {
    protected Routine routine;

    protected ServerJavaRoutineExpression(Routine routine,
                                          List<? extends Expression> children) {
        super(routine.getReturnValue().getType().akType(), children);
        this.routine = routine;
    }

    @Override
    public String name () {
        return routine.getName().toString();
    }

    @Override
    protected void describe(StringBuilder sb) {
        sb.append(name());
    }

    @Override
    public boolean isConstant() {
        return routine.isDeterministic() && super.isConstant();
    }

    @Override
    public boolean nullIsContaminating() {
        return !routine.isCalledOnNullInput();
    }

}
