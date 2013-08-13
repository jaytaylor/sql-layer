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

package com.akiban.qp.rowtype;

import com.akiban.server.explain.*;
import com.akiban.server.expression.Expression;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.texpressions.TPreparedExpression;

import java.util.ArrayList;
import java.util.List;

public class ProjectedRowType extends DerivedRowType
{
    // Object interface

    @Override
    public String toString()
    {
        List<?> projectionsToString = projections == null ? tExprs : projections;
        return String.format("project(%s)", projectionsToString);
    }

    // RowType interface

    @Override
    public int nFields()
    {
        return projections == null ? tInstances.size() : projections.size();
    }

    @Override
    public AkType typeAt(int index) {
        return projections.get(index).valueType();
    }

    @Override
    public TInstance typeInstanceAt(int index) {
        return tInstances.get(index);
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        CompoundExplainer explainer = super.getExplainer(context);
        if (tExprs != null) {
            for (TPreparedExpression expr : tExprs) {
                explainer.addAttribute(Label.EXPRESSIONS, expr.getExplainer(context));
            }
        }
        else {
            for (Expression expr : projections) {
                explainer.addAttribute(Label.EXPRESSIONS, expr.getExplainer(context));
            }
        }
        return explainer;
    }

    // ProjectedRowType interface

    public ProjectedRowType(DerivedTypesSchema schema, int typeId, List<? extends Expression> projections, List<? extends TPreparedExpression> tExpr)
    {
        super(schema, typeId);
        this.projections = projections;
        this.tExprs = tExpr;
        if (tExpr != null) {
            this.tInstances = new ArrayList<>(tExpr.size());
            for (TPreparedExpression expr : tExpr)
                tInstances.add(expr.resultType());
        }
        else {
            this.tInstances = null;
        }
    }
    
    // Object state

    private final List<? extends Expression> projections;
    private final List<? extends TPreparedExpression> tExprs;
    private final List<TInstance> tInstances;
}
