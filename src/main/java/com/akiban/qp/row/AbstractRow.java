/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.qp.row;

import com.akiban.ais.model.UserTable;
import com.akiban.qp.expression.BoundExpressions;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.Quote;
import com.akiban.server.types.ValueSource;
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
    public int compareTo(BoundExpressions row, int leftStartIndex, int rightStartIndex, int fieldCount)
    {
        throw new UnsupportedOperationException();
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
        final int fieldsCount = rowType().nFields();
        AkibanAppender appender = AkibanAppender.of(builder);
        for (int i=0; i < fieldsCount; ++i) {
            if (Types3Switch.ON)
                rowType().typeInstanceAt(i).format(pvalue(i), appender);
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
