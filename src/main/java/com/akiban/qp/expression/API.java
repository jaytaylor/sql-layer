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

import com.akiban.qp.physicaloperator.Bindings;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowBase;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.AkType;
import com.akiban.server.types.FromObjectValueSource;
import com.akiban.server.types.ValueSource;

import java.util.ArrayList;
import java.util.List;

public class API
{
    public static Expression compare(Expression left, Comparison comparison, Expression right)
    {
        return new Compare(left, comparison, right);
    }

    public static Expression field(int position)
    {
        return new Field(position);
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
        return new Literal(value);
    }

    public static Expression variable(int position)
    {
        return new Variable(position);
    }

    public static Expression boundField(int rowPosition, int fieldPosition)
    {
        return new BoundField(rowPosition, fieldPosition);
    }

    public static com.akiban.server.expression.Expression wrap(Expression qpExpression)
    {
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
            throw new UnsupportedOperationException("static typing of old Expressions isn't possible");
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
