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

package com.foundationdb.qp.row;

import com.foundationdb.qp.expression.ExpressionRow;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.ExpressionGenerator;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.explain.*;
import com.foundationdb.server.types3.TPreptimeValue;
import com.foundationdb.server.types3.pvalue.PValueSource;
import com.foundationdb.server.types3.texpressions.TPreparedExpression;
import com.foundationdb.util.AkibanAppender;
import com.foundationdb.util.ArgumentValidation;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public abstract class BindableRow {

    // BindableRow class interface

    public static BindableRow of(RowType rowType, List<? extends ExpressionGenerator> expressions) {
        return of(rowType, API.generateNew(expressions), null);
    }

    public static BindableRow of(RowType rowType,
                                 List<? extends TPreparedExpression> pExpressions,
                                 QueryContext queryContext) {
        Iterator<? extends PValueSource> newVals;
        ArgumentValidation.isEQ("rowType fields", rowType.nFields(), "expressions.size", pExpressions.size());
        for (TPreparedExpression expression : pExpressions) {
            TPreptimeValue tpv = expression.evaluateConstant(queryContext);
            if (tpv == null || tpv.value() == null)
                return new BindingExpressions(rowType, pExpressions);
        }
        newVals = new PExpressionEvaluator(pExpressions, queryContext);
        ImmutableRow holderRow = new ImmutableRow(rowType, newVals);
        return new Delegating(holderRow);
    }

    public static BindableRow of(Row row) {
        return new Delegating(strictCopy(row));
    }

    // BindableRow instance interface

    public abstract Row bind(QueryContext context, QueryBindings bindings);
    public abstract CompoundExplainer getExplainer(ExplainContext context);

    private static ImmutableRow strictCopy(Row input) {
        RowPCopier newCopier;
        newCopier = new RowPCopier(input);
        return new ImmutableRow(input.rowType(), newCopier);
    }

    // nested classes

    private static class BindingExpressions extends BindableRow {
        @Override
        public Row bind(QueryContext context, QueryBindings bindings) {
            return new ExpressionRow(rowType, context, bindings, pExprs);
        }

        @Override
        public CompoundExplainer getExplainer(ExplainContext context) {
            Attributes atts = new Attributes();
            for (TPreparedExpression pexpr : pExprs) {
                atts.put(Label.EXPRESSIONS, pexpr.getExplainer(context));
            }
            return new CompoundExplainer(Type.ROW, atts);
        }

        private BindingExpressions(RowType rowType, List<? extends TPreparedExpression> pExprs)
        {
            this.rowType = rowType;
            this.pExprs = pExprs;
            /*
            if (expressions != null) {
                // TODO do we need an equivalent for pexprs?
                for (Expression expression : expressions) {
                    if (expression.needsRow()) {
                        throw new IllegalArgumentException("expression " + expression + " needs a row");
                    }
                }
            }
            */
        }

        // object interface


        @Override
        public String toString() {
            return "Bindable " + pExprs;
        }

        private final List<? extends TPreparedExpression> pExprs;
        private final RowType rowType;
    }

    private static class RowPCopier implements Iterator<PValueSource>  {

        @Override
        public boolean hasNext() {
            return i < sourceRow.rowType().nFields();
        }

        @Override
        public PValueSource next() {
            return sourceRow.pvalue(i++);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private RowPCopier(Row sourceRow) {
            this.sourceRow = sourceRow;
        }

        private final Row sourceRow;
        private int i = 0;
    }

    private static class PExpressionEvaluator implements Iterator<PValueSource> {

        @Override
        public boolean hasNext() {
            return expressions.hasNext();
        }

        @Override
        public PValueSource next() {
            TPreparedExpression expression = expressions.next();
            TPreptimeValue ptv = expression.evaluateConstant(context);
            assert ptv != null && ptv.value() != null
                    : "not constant: " + expression + " with prepare-time value of " + ptv;
            return ptv.value();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private PExpressionEvaluator(Collection<? extends TPreparedExpression> expressions, QueryContext context)
        {
            this.expressions = expressions.iterator();
            this.context = context;
        }

        private final Iterator<? extends TPreparedExpression> expressions;
        private final QueryContext context;
    }

    private static class Delegating extends BindableRow {
        @Override
        public Row bind(QueryContext context, QueryBindings bindings) {
            return row;
        }

        @Override
        public CompoundExplainer getExplainer(ExplainContext context) {
            Attributes atts = new Attributes();
            for (int i = 0; i < row.rowType().nFields(); i++) {
                atts.put(Label.EXPRESSIONS, PrimitiveExplainer.getInstance(formatAsLiteral(i)));
            }
            return new CompoundExplainer(Type.ROW, atts);
        }

        private String formatAsLiteral(int i) {
            StringBuilder str = new StringBuilder();
            row.rowType().typeInstanceAt(i).formatAsLiteral(row.pvalue(i), AkibanAppender.of(str));
            return str.toString();
        }

        @Override
        public String toString() {
            return String.valueOf(row);
        }

        private Delegating(ImmutableRow row) {
            this.row = row;
        }

        private final ImmutableRow row;
    }
}
