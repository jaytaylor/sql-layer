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

package com.foundationdb.qp.expression;

import java.util.ArrayList;
import java.util.List;

import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.row.AbstractRow;
import com.foundationdb.qp.row.HKey;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.expression.ExpressionEvaluation;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.types3.pvalue.PValueSource;
import com.foundationdb.server.types3.texpressions.TEvaluatableExpression;
import com.foundationdb.server.types3.texpressions.TPreparedExpression;

public class ExpressionRow extends AbstractRow
{
    private RowType rowType;
    private List<? extends Expression> expressions;
    private List<ExpressionEvaluation> evaluations;
    private List<? extends TPreparedExpression> pExpressions;
    private List<TEvaluatableExpression> pEvaluations;

    public ExpressionRow(RowType rowType, QueryContext context, QueryBindings bindings, List<? extends Expression> expressions,
                         List<? extends TPreparedExpression> pExpressions) {
        this.rowType = rowType;
        this.expressions = expressions;
        this.pExpressions = pExpressions;
        if (pExpressions != null) {
            assert expressions == null : "can't have both types be non-null";
            this.pEvaluations = new ArrayList<>(pExpressions.size());
            for (TPreparedExpression expression : pExpressions) {
                TEvaluatableExpression evaluation = expression.build();
                evaluation.with(context);
                evaluation.with(bindings);
                this.pEvaluations.add(evaluation);
            }
        }
        else if (expressions != null) {
            this.evaluations = new ArrayList<>(expressions.size());
            for (Expression expression : expressions) {
                if (expression.needsRow()) {
                    throw new AkibanInternalException("expression needed a row: " + expression + " in " + expressions);
                }
                ExpressionEvaluation evaluation = expression.evaluation();
                evaluation.of(context);
                evaluation.of(bindings);
                this.evaluations.add(evaluation);
            }
        }
        else
            throw new AssertionError("can't have both lists be null");
    }

    /* AbstractRow */

    @Override
    public RowType rowType() {
        return rowType;
    }

    @Override
    public ValueSource eval(int i) {
        return evaluations.get(i).eval();
    }

    @Override
    public PValueSource pvalue(int i) {
        TEvaluatableExpression eval = pEvaluations.get(i);
        eval.evaluate();
        return eval.resultValue();
    }

    @Override
    public HKey hKey() {
        throw new UnsupportedOperationException();        
    }

    @Override
    public void release() {
    }

    @Override
    public boolean isShared() {
        return false;
    }

    @Override
    public void acquire() {
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
            Object expression = (pExpressions != null) ? pExpressions.get(i) : expressions.get(i);
            if (expression != null)
                str.append(expression);
        }
        str.append(']');
        return str.toString();
    }
}
