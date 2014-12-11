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

import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.util.AkibanAppender;

public abstract class AbstractRow implements Row
{
    // ValueRecord interface

    /**
     * Compares two rows and indicates if and where they differ.
     * @param row The row to be compared to this one.
     * @param leftStartIndex First field to compare in this row.
     * @param rightStartIndex First field to compare in the other row.
     * @param fieldCount Number of fields to compare.
     * @return 0 if all fields are equal. A negative value indicates that this row ranks lower than the other row.
     * A positive value indicates that the other row ranks lower. In both non-zero cases, the absolute value
     * of the return value is the position of the field that differed, starting the numbering at 1.
     * E.g. a return value of -2 means that the first fields of the rows match, and that in the second field,
     * this row had the smaller value.
     */
    @Override
    public int compareTo(Row row, int leftStartIndex, int rightStartIndex, int fieldCount)
    {
        for (int i = 0; i < fieldCount; i++) {
            TInstance leftType = rowType().typeAt(leftStartIndex + i);
            ValueSource leftValue = value(leftStartIndex + i);
            TInstance rightType = ((Row)row).rowType().typeAt(rightStartIndex + i);
            ValueSource rightValue = row.value(rightStartIndex + i);
            int c = TClass.compare(leftType, leftValue, rightType, rightValue);
            if (c != 0) return (c < 0) ? -(i + 1) : (i + 1);
        }
        return 0;
    }

    // Row interface

    @Override
    public abstract RowType rowType();

    @Override
    public abstract HKey hKey();

    @Override
    public final ValueSource value(int i) {
        return checkValueType(i, uncheckedValue(i));
    }

    protected abstract ValueSource uncheckedValue(int i);
    
    @Override
    public HKey ancestorHKey(Table table)
    {
        throw new UnsupportedOperationException(getClass().toString());
    }

    @Override
    public final boolean ancestorOf(Row that)
    {
        return this.hKey().prefixOf(that.hKey());
    }

    @Override
    public boolean containsRealRowOf(Table table)
    {
        throw new UnsupportedOperationException(getClass().toString());
    }

    @Override
    public Row subRow(RowType subRowType)
    {
        return rowType() == subRowType ? this : null;
    }

    @Override
    public boolean isBindingsSensitive() {
        return true;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(this.getClass().getSimpleName()).append('[');
        RowType rowType = rowType();
        final int fieldsCount = rowType.nFields();
        AkibanAppender appender = AkibanAppender.of(builder);
        for (int i=0; i < fieldsCount; ++i) {
            if (rowType.typeAt(i) == null) {
                assert value(i).isNull();
                builder.append("NULL");
            }
            else {
                if (value(i).hasAnyValue()) {
                    rowType.typeAt(i).format(value(i), appender);
                } else {
                    builder.append("Unset");
                }
            }
            if(i+1 < fieldsCount) {
                builder.append(',').append(' ');
            }
        }
        builder.append(']');
        return builder.toString();
    }

    // For use by this class

    private ValueSource checkValueType(int i, ValueSource nextValue) {
        if (DEBUG_ROWTYPE) {
            TInstance nextValueType = nextValue.getType();
            TInstance expectedTInst = rowType().typeAt(i);
            if (expectedTInst == null && !nextValue.isNull())
                throw new RowType.InconsistentRowTypeException(i, nextValue.getObject());
            if (TInstance.tClass(nextValueType) != TInstance.tClass(expectedTInst))
                throw new RowType.InconsistentRowTypeException(i, expectedTInst, nextValueType, nextValue);
        }
        return nextValue;
    }
}
