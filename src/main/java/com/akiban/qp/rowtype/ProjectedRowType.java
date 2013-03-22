
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
