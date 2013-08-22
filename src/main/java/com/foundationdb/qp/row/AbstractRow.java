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

import com.foundationdb.ais.model.UserTable;
import com.foundationdb.qp.expression.BoundExpressions;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.Quote;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.types.util.ValueSources;
import com.foundationdb.server.types3.TClass;
import com.foundationdb.server.types3.TInstance;
import com.foundationdb.server.types3.Types3Switch;
import com.foundationdb.server.types3.pvalue.PValueSource;
import com.foundationdb.server.types3.pvalue.PValueSources;
import com.foundationdb.util.AkibanAppender;

public abstract class AbstractRow implements Row
{
    // BoundExpressions interface

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
    public int compareTo(RowBase row, int leftStartIndex, int rightStartIndex, int fieldCount)
    {
        if (Types3Switch.ON) {
            for (int i = 0; i < fieldCount; i++) {
                TInstance leftType = rowType().typeInstanceAt(leftStartIndex + i);
                PValueSource leftValue = pvalue(leftStartIndex + i);
                TInstance rightType = ((Row)row).rowType().typeInstanceAt(rightStartIndex + i);
                PValueSource rightValue = row.pvalue(rightStartIndex + i);
                int c = TClass.compare(leftType, leftValue, rightType, rightValue);
                if (c != 0) return (c < 0) ? -(i + 1) : (i + 1);
            }
        }
        else {
            for (int i = 0; i < fieldCount; i++) {
                ValueSource leftValue = eval(leftStartIndex + i);
                ValueSource rightValue = row.eval(rightStartIndex + i);
                long c = ValueSources.compare(leftValue, rightValue, null);
                if (c != 0) return (c < 0) ? -(i + 1) : (i + 1);
            }
        }
        return 0;
    }

    // RowBase interface

    @Override
    public abstract RowType rowType();

    @Override
    public abstract HKey hKey();

    @Override
    public PValueSource pvalue(int i) {
        // Default, though inefficient.
        return PValueSources.fromValueSource(eval(i), rowType().typeInstanceAt(i));
    }

    @Override
    public ValueSource eval(int index) {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public HKey ancestorHKey(UserTable table)
    {
        throw new UnsupportedOperationException(getClass().toString());
    }

    @Override
    public final boolean ancestorOf(RowBase that)
    {
        return this.hKey().prefixOf(that.hKey());
    }

    @Override
    public boolean containsRealRowOf(UserTable userTable)
    {
        throw new UnsupportedOperationException(getClass().toString());
    }

    @Override
    public Row subRow(RowType subRowType)
    {
        return rowType() == subRowType ? this : null;
    }

    // Shareable interface

    @Override
    public void acquire()
    {
        assert references >= 0 : this;
        beforeAcquire();
        references++;
    }

    @Override
    public boolean isShared()
    {
        assert references >= 0 : this;
        return references > 1;
    }

    @Override
    public void release()
    {
        assert references > 0 : this;
        --references;
        afterRelease();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(this.getClass().getSimpleName()).append('[');
        RowType rowType = rowType();
        final int fieldsCount = rowType.nFields();
        AkibanAppender appender = AkibanAppender.of(builder);
        for (int i=0; i < fieldsCount; ++i) {
            if (Types3Switch.ON) {
                if (rowType.typeInstanceAt(i) == null) {
                    assert pvalue(i).isNull();
                    builder.append("NULL");
                }
                else {
                    rowType.typeInstanceAt(i).format(pvalue(i), appender);
                }
            }
            else
                eval(i).appendAsString(appender, Quote.SINGLE_QUOTE);
            if(i+1 < fieldsCount) {
                builder.append(',').append(' ');
            }
        }
        builder.append(']');
        return builder.toString();
    }

    public RowData rowData() {
        throw new UnsupportedOperationException();
    }

    // for use by subclasses
    protected void afterRelease() {}
    protected void beforeAcquire() {}

    // Object state

    private int references = 0;
}
