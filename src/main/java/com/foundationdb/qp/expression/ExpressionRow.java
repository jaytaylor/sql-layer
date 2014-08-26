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

package com.foundationdb.qp.expression;

import java.util.ArrayList;
import java.util.List;

import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.row.AbstractRow;
import com.foundationdb.qp.row.HKey;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.texpressions.TEvaluatableExpression;
import com.foundationdb.server.types.texpressions.TPreparedExpression;

public class ExpressionRow extends AbstractRow
{
    private RowType rowType;
    private List<? extends TPreparedExpression> pExpressions;
    private List<TEvaluatableExpression> pEvaluations;

    public ExpressionRow(RowType rowType, QueryContext context, QueryBindings bindings, 
                         List<? extends TPreparedExpression> pExpressions) {
        this.rowType = rowType;
        this.pExpressions = pExpressions;
        assert pExpressions != null : "pExpressions can not be null";
        this.pEvaluations = new ArrayList<>(pExpressions.size());
        for (TPreparedExpression expression : pExpressions) {
            TEvaluatableExpression evaluation = expression.build();
            evaluation.with(context);
            evaluation.with(bindings);
            this.pEvaluations.add(evaluation);
        }
    }

    /* AbstractRow */

    @Override
    public RowType rowType() {
        return rowType;
    }

    @Override
    public ValueSource value(int i) {
        TEvaluatableExpression eval = pEvaluations.get(i);
        eval.evaluate();
        return eval.resultValue();
    }

    @Override
    public HKey hKey() {
        throw new UnsupportedOperationException();        
    }

    /* Object */

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append(getClass().getSimpleName());
        str.append('[');
        int nf = rowType().nFields();
        for (int i = 0; i < nf; i++) {
            if (i > 0) str.append(", ");
            Object expression = pExpressions.get(i);
            if (expression != null)
                str.append(expression);
        }
        str.append(']');
        return str.toString();
    }

   @Override
   public boolean isBindingsSensitive() {
       return false;
   }
}
