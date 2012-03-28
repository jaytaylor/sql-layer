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
import com.akiban.util.AkibanAppender;
import com.akiban.util.ShareHolder;

public abstract class AbstractRow implements Row
{
    // BoundExpressions interface

    @Override
    public int compareTo(BoundExpressions row, int leftStartIndex, int rightStartIndex, int fieldCount)
    {
        throw new UnsupportedOperationException();
    }

    // Row interface

    @Override
    public abstract RowType rowType();

    @Override
    public abstract HKey hKey();

    @Override
    public final boolean ancestorOf(RowBase that)
    {
        return this.hKey().prefixOf(that.hKey());
    }

    @Override
    public boolean containsRealRowOf(UserTable userTable) {
        return false;
    }

    @Override
    public Row subRow(RowType subRowType)
    {
        return rowType() == subRowType ? this : null;
    }

    // Row interface

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

    protected static boolean containRealRowOf(
            ShareHolder<? extends Row> one,
            ShareHolder<? extends Row> two,
            UserTable rowType
    ) {
        return     (one.isHolding() && one.get().rowType().hasUserTable() && one.get().rowType().userTable() == rowType)
                || (two.isHolding() && two.get().rowType().hasUserTable() && two.get().rowType().userTable() == rowType)
                || (one.isHolding() && one.get().containsRealRowOf(rowType))
                || (two.isHolding() && two.get().containsRealRowOf(rowType))
                ;
    }
    // Object state

    private int references = 0;
}
