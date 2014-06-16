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

package com.foundationdb.qp.rowtype;

import com.foundationdb.server.explain.*;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.util.ArgumentValidation;

import java.util.ArrayList;
import java.util.List;

public class ProjectedRowType extends DerivedRowType
{
    // Object interface

    @Override
    public String toString()
    {
        List<?> projectionsToString = tExprs;
        return String.format("project(%s)", projectionsToString);
    }

    // RowType interface

    @Override
    public int nFields()
    {
        return tInstances.size();
    }

    @Override
    public TInstance typeAt(int index) {
        return tInstances.get(index);
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        CompoundExplainer explainer = super.getExplainer(context);
        for (TPreparedExpression expr : tExprs) {
            explainer.addAttribute(Label.EXPRESSIONS, expr.getExplainer(context));
        }
        return explainer;
    }

    // ProjectedRowType interface

    public ProjectedRowType(Schema schema, int typeId, List<? extends TPreparedExpression> tExpr)
    {
        super(schema, typeId);
        ArgumentValidation.notNull("tExpressions", tExpr);
        this.tExprs = tExpr;
        this.tInstances = new ArrayList<>(tExpr.size());
        for (TPreparedExpression expr : tExpr)
            tInstances.add(expr.resultType());
    }

    protected List<? extends TPreparedExpression> getExpressions() {
        return tExprs;
    }

    // Object state

    private final List<? extends TPreparedExpression> tExprs;
    private final List<TInstance> tInstances;
}
