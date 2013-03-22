
package com.akiban.server.types3.texpressions;

import com.akiban.qp.operator.Operator;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.explain.*;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.aksql.aktypes.AkBool;
import com.akiban.server.types3.pvalue.PValueTarget;

public class ExistsSubqueryTExpression extends SubqueryTExpression
{
    private static final class InnerEvaluation extends SubqueryTEvaluateble
    {
        InnerEvaluation(Operator subquery,
                        RowType outerRowType, RowType innerRowType,
                        int bindingPosition)
        {
            super(subquery, outerRowType, innerRowType, bindingPosition, AkBool.INSTANCE.instance(true));
        }

        @Override
        protected void doEval(PValueTarget out)
        {   
            out.putBool(next() != null);
        }
    }

    public ExistsSubqueryTExpression(Operator subquery, RowType outerRowType, 
                                     RowType innerRowType, int bindingPosition)
    {
        super(subquery, outerRowType, innerRowType, bindingPosition);
    }
    
    @Override
    public TInstance resultType()
    {
        return AkBool.INSTANCE.instance(true);
    }

    @Override
    public TEvaluatableExpression build()
    {
        return new InnerEvaluation(subquery(),
                                   outerRowType(), innerRowType(),
                                   bindingPosition());
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context) {
        CompoundExplainer explainer = super.getExplainer(context);
        explainer.addAttribute(Label.NAME, PrimitiveExplainer.getInstance("EXISTS"));
        return explainer;
    }

}
