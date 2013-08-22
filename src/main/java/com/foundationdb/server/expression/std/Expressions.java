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

package com.foundationdb.server.expression.std;

import com.foundationdb.ais.model.Column;
import com.foundationdb.qp.expression.IndexBound;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.row.RowBase;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.FromObjectValueSource;
import com.foundationdb.server.types.ValueSource;

public class Expressions
{
    public static Expression field(Column column, int position)
    {
        return new ColumnExpression(column, position);
    }

    public static Expression compare(Expression left, Comparison comparison, Expression right)
    {
        return new CompareExpression(left, comparison, right);
    }

    public static Expression collate(Expression left, Comparison comparison, Expression right, AkCollator collator)
    {
        return new CompareExpression(left, comparison, right, collator);
    }

    public static Expression field(RowType rowType, int position)
    {
        return new FieldExpression(rowType, position);
    }

    public static IndexBound indexBound(RowBase row, ColumnSelector columnSelector)
    {
        return new IndexBound(row, columnSelector);
    }

    public static IndexKeyRange indexKeyRange(IndexRowType indexRowType, IndexBound lo, boolean loInclusive, IndexBound hi, boolean hiInclusive)
    {
        return IndexKeyRange.bounded(indexRowType, lo, loInclusive, hi, hiInclusive);
    }

    public static Expression literal(Object value)
    {
        return new LiteralExpression(new FromObjectValueSource().setReflectively(value));
    }

    public static Expression literal(Object value, AkType type)
    {
        return new LiteralExpression(new FromObjectValueSource().setExplicitly(value, type));
    }

    public static Expression variable(AkType type, int position)
    {
        return new VariableExpression(type, position);
    }

    public static Expression valueSource(ValueSource valueSource)
    {
        return new ValueSourceExpression(valueSource);
    }

    public static Expression boundField(RowType rowType, int rowPosition, int fieldPosition)
    {
        return new BoundFieldExpression(rowPosition, new FieldExpression(rowType, fieldPosition));
    }
}
