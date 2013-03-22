
package com.akiban.qp.row;

import com.akiban.ais.model.UserTable;
import com.akiban.qp.expression.BoundExpressions;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.Quote;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueSources;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.Types3Switch;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueSources;
import com.akiban.util.AkibanAppender;

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

    // for use by subclasses
    protected void afterRelease() {}
    protected void beforeAcquire() {}

    // Object state

    private int references = 0;
}
