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

    @Override
    public void reset()
    {
        // does nothing
    }
    
    private static final class InnerEvaluation extends SubqueryTEvaluateble
    {
        private final TPreparedExpression expression;
        
        public InnerEvaluation(Operator subquery,
                               TPreparedExpression expression,
                               RowType outerRowType, RowType innerRowType,
                               int bindingPosition)
        {
            super(subquery, outerRowType, innerRowType, bindingPosition, expression.resultType().typeClass().underlyingType());
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
