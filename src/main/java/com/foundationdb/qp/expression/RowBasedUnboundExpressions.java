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

import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.ExpressionGenerator;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.explain.Attributes;
import com.foundationdb.server.explain.CompoundExplainer;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.Label;
import com.foundationdb.server.explain.Type;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.types3.pvalue.PValueSource;
import com.foundationdb.server.types3.texpressions.TPreparedExpression;

import java.util.List;

public final class RowBasedUnboundExpressions implements UnboundExpressions {
    @Override
    public BoundExpressions get(QueryContext context, QueryBindings bindings) {
        return new ExpressionsAndBindings(rowType, expressions, pExprs, context, bindings);
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context) {
        Attributes atts = new Attributes();
        if (pExprs != null) {
            for (TPreparedExpression expression : pExprs) {
                atts.put(Label.EXPRESSIONS, expression.getExplainer(context));
            }
        }
        else {
            for (Expression expression : expressions) {
                atts.put(Label.EXPRESSIONS, expression.getExplainer(context));
            }
        }
        return new CompoundExplainer(Type.ROW, atts);
    }

    @Override
    public String toString() {
        return "UnboundExpressions" + (expressions == null ? pExprs : expressions);
    }

    @Deprecated
    public RowBasedUnboundExpressions(RowType rowType, List<ExpressionGenerator> expressions) {
        this(rowType, API.generateOld(expressions), API.generateNew(expressions));
    }

    public RowBasedUnboundExpressions(RowType rowType, List<Expression> expressions, List<TPreparedExpression> pExprs)
    {
        if (expressions != null) {
            assert pExprs == null : "both can't be non-null";
            for (Expression expression : expressions) {
                if (expression == null) {
                    throw new IllegalArgumentException();
                }
            }
        }
        else if (pExprs != null) {
            for (TPreparedExpression expression : pExprs) {
                if (expression == null) {
                    throw new IllegalArgumentException();
                }
            }
        }
        else
            assert false : "both can't be null";
        this.expressions = expressions;
        this.pExprs = pExprs;
        this.rowType = rowType;
    }

    private final List<Expression> expressions;
    private final List<TPreparedExpression> pExprs;
    private final RowType rowType;

    private static class ExpressionsAndBindings implements BoundExpressions {

        @Override
        public ValueSource eval(int index) {
            return expressionRow.eval(index);
        }

        @Override
        public PValueSource pvalue(int index) {
            return expressionRow.pvalue(index);
        }

        ExpressionsAndBindings(RowType rowType, List<Expression> expressions, List<TPreparedExpression> pExprs,
                               QueryContext context, QueryBindings bindings)
        {
            expressionRow = new ExpressionRow(rowType, context, bindings, expressions, pExprs);
        }

        private final ExpressionRow expressionRow;
    }
}
