/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.expression.std;

import com.akiban.server.expression.Expression;
import com.akiban.server.types.AkType;
import com.akiban.sql.optimizer.explain.Explainer;
import com.akiban.sql.optimizer.explain.Type;
import com.akiban.sql.optimizer.explain.std.ExpressionExplainer;
import java.util.List;

public abstract class AbstractNoArgExpression implements Expression {

    // Expression interface
    
    @Override
    public Explainer getExplainer ()
    {
        return new ExpressionExplainer(Type.FUNCTION, name(), (List)null);
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
