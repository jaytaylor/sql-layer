/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.qp.rowtype;

import com.akiban.server.explain.*;
import com.akiban.server.expression.Expression;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.texpressions.TPreparedExpression;
import com.akiban.server.types3.texpressions.TPreparedExpressions;

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
                explainer.addAttribute(Label.EXPRESSIONS, TPreparedExpressions.getExplainer(expr));
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
            this.tInstances = new ArrayList<TInstance>(tExpr.size());
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
