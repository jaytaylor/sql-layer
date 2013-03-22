
package com.akiban.server.types3.texpressions;

import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.error.SubqueryTooManyRowsException;
import com.akiban.server.explain.*;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.pvalue.PValueTargets;

public class ScalarSubqueryTExpression extends SubqueryTExpression
{
    private static final class InnerEvaluation extends SubqueryTEvaluateble
    {
        private final TPreparedExpression expression;
        
        public InnerEvaluation(Operator subquery,
                               TPreparedExpression expression,
                               RowType outerRowType, RowType innerRowType,
                               int bindingPosition)
        {
            super(subquery, outerRowType, innerRowType, bindingPosition, expression.resultType());
            this.expression = expression;
        }

        @Override
        protected void doEval(PValueTarget out)
        {
            Row row = next();
            if (row == null)
                out.putNull();
            else
            {
                TEvaluatableExpression eval = expression.build();
                
                eval.with(queryContext());
                eval.with(row);

                // evaluate the result
                eval.evaluate();
                PValueTargets.copyFrom(eval.resultValue(), out);
                
                if (next() != null)
                    throw new SubqueryTooManyRowsException();
            }
        }
    }

    private final TPreparedExpression expression;
    
    public ScalarSubqueryTExpression(Operator subquery,
                                     TPreparedExpression expression,
                                     RowType outerRowType, RowType innerRowType, 
                                     int bindingPosition)
    {
        super(subquery, outerRowType, innerRowType, bindingPosition);
        this.expression = expression;
    }

    @Override
    public TInstance resultType()
    {
        return expression.resultType();
    }

    @Override
    public TEvaluatableExpression build()
    {
        return new InnerEvaluation(subquery(),
                                   expression,
                                   outerRowType(), innerRowType(),
                                   bindingPosition());
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context) {
        CompoundExplainer explainer = super.getExplainer(context);
        explainer.addAttribute(Label.NAME, PrimitiveExplainer.getInstance("VALUE"));
        explainer.addAttribute(Label.EXPRESSIONS, expression.getExplainer(context));
        return explainer;
    }
}
