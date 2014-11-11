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

package com.foundationdb.server.types.texpressions;

import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.error.SubqueryTooManyRowsException;
import com.foundationdb.server.explain.*;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.value.ValueTargets;

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
        protected void doEval(ValueTarget out)
        {
            Row row = next();
            if (row == null)
                out.putNull();
            else
            {
                TEvaluatableExpression eval = expression.build();
                
                eval.with(queryContext());
                eval.with(queryBindings());
                eval.with(row);

                // evaluate the result
                eval.evaluate();
                ValueTargets.copyFrom(eval.resultValue(), out);
                
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
