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

package com.akiban.server.expression.std;

import com.akiban.ais.model.Column;
import com.akiban.qp.expression.*;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.types.AkType;
import com.akiban.server.types.FromObjectValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Expressions
{
    public static Expression field(Column column, int position)
    {
        return new NewExpressionWrapper(new ColumnExpression(column, position));
    }

    public static Expression compare(Expression left, com.akiban.qp.expression.Comparison comparison, Expression right)
    {
        CompareExpression compExpr = new CompareExpression(wrapAll(left, right), comparison.newStyle());
        return new NewExpressionWrapper(compExpr);
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

    public static Expression variable(AkType type, int position)
    {
        return new NewExpressionWrapper(new VariableExpression(type, position));
    }

    public static Expression boundField(RowType rowType, int rowPosition, int fieldPosition)
    {
        return new NewExpressionWrapper(
                new BoundFieldExpression(rowPosition, new FieldExpression(rowType, fieldPosition))
        );
    }

    public static com.akiban.server.expression.Expression wrap(Expression qpExpression)
    {
        if (qpExpression == null)
            throw new IllegalArgumentException();
        return qpExpression.get();
    }

    public static List<? extends com.akiban.server.expression.Expression> wrapAll(List<? extends Expression> list)
    {
        if (list == null)
            throw new IllegalArgumentException();
        List<com.akiban.server.expression.Expression> result = new ArrayList<com.akiban.server.expression.Expression>();
        for (Expression qpExpression : list) {
            result.add(wrap(qpExpression));
        }
        return result;
    }
    public static List<? extends com.akiban.server.expression.Expression> wrapAll(Expression... qpExpressions)
    {
        return wrapAll(Arrays.asList(qpExpressions));
    }

    public static com.akiban.qp.expression.Comparison EQ = com.akiban.qp.expression.Comparison.EQ;
    public static com.akiban.qp.expression.Comparison NE = com.akiban.qp.expression.Comparison.NE;
    public static com.akiban.qp.expression.Comparison LT = com.akiban.qp.expression.Comparison.LT;
    public static com.akiban.qp.expression.Comparison LE = com.akiban.qp.expression.Comparison.LE;
    public static com.akiban.qp.expression.Comparison GT = com.akiban.qp.expression.Comparison.GT;
    public static com.akiban.qp.expression.Comparison GE = com.akiban.qp.expression.Comparison.GE;
}
