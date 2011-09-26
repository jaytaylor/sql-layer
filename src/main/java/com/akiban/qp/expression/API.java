/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.qp.expression;

import com.akiban.qp.operator.Bindings;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.expression.std.BoundFieldExpression;
import com.akiban.server.expression.std.CompareExpression;
import com.akiban.server.expression.std.FieldExpression;
import com.akiban.server.expression.std.LiteralExpression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.AkType;
import com.akiban.server.types.FromObjectValueSource;
import com.akiban.server.types.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class API
{
    public static Expression compare(Expression left, Comparison comparison, Expression right)
    {
        CompareExpression compExpr = new CompareExpression(wrapAll(left, right), comparison.newStyle());
        return new NewExpressionWrapper(compExpr);
    }

    @Deprecated
    public static Expression field(int position)
    {
        throw new UnsupportedOperationException("need rowtype for position " + position);
    }

    public static Expression field(RowType rowType, int position)
    {
        return new NewExpressionWrapper(new FieldExpression(rowType, position));
    }

    public static IndexBound indexBound(RowBase row, ColumnSelector columnSelector)
    {
        return new IndexBound(row, columnSelector);
    }

    public static IndexKeyRange indexKeyRange(IndexBound lo, boolean loInclusive, IndexBound hi, boolean hiInclusive)
    {
        return new IndexKeyRange(lo, loInclusive, hi, hiInclusive);
    }

    public static Expression literal(Object value)
    {
        return new NewExpressionWrapper(new LiteralExpression(new FromObjectValueSource().setReflectively(value)));
    }

    public static Expression variable(int position)
    {
        return new Variable(position);
    }

    public static Expression boundField(RowType rowType, int rowPosition, int fieldPosition)
    {
        return new NewExpressionWrapper(
                new BoundFieldExpression(rowPosition, new FieldExpression(rowType, fieldPosition))
        );
    }

    public static com.akiban.server.expression.Expression wrap(Expression qpExpression)
    {
        if (qpExpression == null) {
            throw new IllegalArgumentException();
        }
        return new InnerNewExpressionWrapper(qpExpression);
    }

    public static List<? extends com.akiban.server.expression.Expression> wrapAll
        (List<? extends Expression> qpExpressions)
    {
        if (qpExpressions == null) {
            throw new IllegalArgumentException();
        }
        List<com.akiban.server.expression.Expression> result = new ArrayList<com.akiban.server.expression.Expression>();
        for (Expression qpExpression : qpExpressions) {
            if (qpExpression == null) {
                throw new IllegalArgumentException();
            }
            result.add(wrap(qpExpression));
        }
        return result;
    }
    public static List<? extends com.akiban.server.expression.Expression> wrapAll(Expression... qpExpressions)
    {
        return wrapAll(Arrays.asList(qpExpressions));
    }

    public static Comparison EQ = Comparison.EQ;
    public static Comparison NE = Comparison.NE;
    public static Comparison LT = Comparison.LT;
    public static Comparison LE = Comparison.LE;
    public static Comparison GT = Comparison.GT;
    public static Comparison GE = Comparison.GE;

    // Inner classes

    private static class InnerNewExpressionWrapper implements com.akiban.server.expression.Expression
    {
        // Expression interface

        @Override
        public boolean isConstant()
        {
            return false;
        }

        @Override
        public boolean needsBindings()
        {
            return true;
        }

        @Override
        public boolean needsRow()
        {
            return true;
        }

        @Override
        public ExpressionEvaluation evaluation()
        {
            return new NewExpressionEvaluationWrapper(delegate);
        }

        @Override
        public AkType valueType()
        {
            return delegate.getAkType();
        }

        // Object interface

        @Override
        public String toString()
        {
            return delegate.toString();
        }

        private InnerNewExpressionWrapper(Expression delegate)
        {
            this.delegate = delegate;
        }

        private final Expression delegate;
    }

    private static class NewExpressionEvaluationWrapper implements ExpressionEvaluation
    {
        @Override
        public String toString()
        {
            return delegate.toString();
        }

        @Override
        public void of(Row row)
        {
            this.row = row;
        }

        @Override
        public void of(Bindings bindings)
        {
            this.bindings = bindings;
        }

        @Override
        public ValueSource eval()
        {
            Object wrapped = delegate.evaluate(row, bindings);
            source.setReflectively(wrapped);
            return source;
        }

        @Override
        public void acquire() {
            row.acquire();
        }

        @Override
        public boolean isShared() {
            return row.isShared();
        }

        @Override
        public void release() {
            row.release();
        }

        private NewExpressionEvaluationWrapper(Expression delegate)
        {
            this.delegate = delegate;
            this.source = new FromObjectValueSource();
        }

        private final Expression delegate;
        private final FromObjectValueSource source;
        private Row row;
        private Bindings bindings;
    }
}
