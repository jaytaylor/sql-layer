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

import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.expression.std.BoundFieldExpression;
import com.akiban.server.expression.std.CompareExpression;
import com.akiban.server.expression.std.FieldExpression;
import com.akiban.server.expression.std.LiteralExpression;
import com.akiban.server.expression.std.VariableExpression;
import com.akiban.server.types.AkType;
import com.akiban.server.types.FromObjectValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class API
{
    @Deprecated
    public static Expression field(int position)
    {
        return field(null, position);
//        throw new UnsupportedOperationException("need rowtype for position " + position);
    }

    public static Expression compare(Expression left, Comparison comparison, Expression right)
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
        return qpExpression.get();
    }

    public static List<? extends com.akiban.server.expression.Expression> wrapAll(List<? extends Expression> list)
    {
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

    public static Comparison EQ = Comparison.EQ;
    public static Comparison NE = Comparison.NE;
    public static Comparison LT = Comparison.LT;
    public static Comparison LE = Comparison.LE;
    public static Comparison GT = Comparison.GT;
    public static Comparison GE = Comparison.GE;
}
