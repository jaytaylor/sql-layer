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

package com.foundationdb.server.expression.std;

import com.foundationdb.qp.exec.Plannable;
import com.foundationdb.server.explain.CompoundExplainer;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.Type;
import com.foundationdb.server.explain.std.ExpressionExplainer;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.types.AkType;
import java.util.List;
import java.util.Map;

public abstract class AbstractNoArgExpression implements Expression {

    // Expression interface
    
    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        return new ExpressionExplainer(Type.FUNCTION, name(), context);
    }
    
    @Override
    public boolean nullIsContaminating()
    {
        return true;
    }
    
    @Override
    public boolean isConstant() {
        return true;
    }

    @Override
    public boolean needsBindings() {
        return false;
    }

    @Override
    public boolean needsRow() {
        return false;
    }

    @Override
    public AkType valueType() {
        return type;
    }

    // for use by subclasses

    protected AbstractNoArgExpression(AkType type) {
        this.type = type;
    }

    // object interface

    @Override
    public String toString() {
        return name() + "()";
    }

    // object state

    private final AkType type;
}
