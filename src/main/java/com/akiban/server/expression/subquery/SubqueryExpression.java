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

package com.akiban.server.expression.subquery;

import com.akiban.qp.operator.Operator;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.explain.*;
import com.akiban.server.expression.Expression;
import com.akiban.util.ArgumentValidation;
import java.util.Map;

public abstract class SubqueryExpression implements Expression {

    @Override
    public boolean nullIsContaminating() {
        return true;
    }
    
    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean needsBindings() {
        return true;
    }

    @Override
    public boolean needsRow() {
        return (outerRowType != null);
    }
    
    @Override
    public CompoundExplainer getExplainer(ExplainContext context) {
        Attributes states = new Attributes();
        states.put(Label.NAME, PrimitiveExplainer.getInstance(name()));
        states.put(Label.OPERAND, subquery.getExplainer(context)); 
        states.put(Label.OUTER_TYPE, outerRowType.getExplainer(context));
        states.put(Label.INNER_TYPE, innerRowType.getExplainer(context));
        states.put(Label.BINDING_POSITION, PrimitiveExplainer.getInstance(bindingPosition));
        return new CompoundExplainer(Type.SUBQUERY, states);
    }

    // for use by subclasses

    protected final Operator subquery() {
        return subquery;
    }
    protected final RowType outerRowType() {
        return outerRowType;
    }
    protected final RowType innerRowType() {
        return innerRowType;
    }
    protected final int bindingPosition() {
        return bindingPosition;
    }

    protected SubqueryExpression(Operator subquery, RowType outerRowType, 
                                 RowType innerRowType, int bindingPosition) {
        ArgumentValidation.notNull("subquery", subquery);
        ArgumentValidation.isGTE("binding position", bindingPosition, 0);
        this.subquery = subquery;
        this.outerRowType = outerRowType;
        this.innerRowType = innerRowType;
        this.bindingPosition = bindingPosition;
    }

    private final Operator subquery;
    private final RowType outerRowType;
    private final RowType innerRowType;
    private final int bindingPosition;

}
