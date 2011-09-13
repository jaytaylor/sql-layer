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
import com.akiban.server.api.dml.ColumnSelector;

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
    
    public static Comparison EQ = Comparison.EQ;
    public static Comparison NE = Comparison.NE;
    public static Comparison LT = Comparison.LT;
    public static Comparison LE = Comparison.LE;
    public static Comparison GT = Comparison.GT;
    public static Comparison GE = Comparison.GE;
}
