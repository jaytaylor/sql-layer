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

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Index;
import com.akiban.qp.expression.BoundExpressions;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.collation.AkCollator;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.store.PersistitKeyAppender;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types3.TInstance;
import com.persistit.Key;
import com.persistit.exception.PersistitException;

public abstract class IndexRow extends AbstractRow
{
    // BoundExpressions interface

    @Override
    public ValueSource eval(int index)
    {
        throw new UnsupportedOperationException();
    }

    public int compareTo(BoundExpressions row, int leftStartIndex, int rightStartIndex, int fieldCount)
    {
        throw new UnsupportedOperationException();
    }

    // RowBase interface

    public RowType rowType()
    {
        throw new UnsupportedOperationException();
    }

    public HKey hKey()
    {
        throw new UnsupportedOperationException();
    }

    // IndexRow interface

    public abstract void initialize(RowData rowData, Key hKey);

    public final void append(Column column, ValueSource source)
    {
        append(source, column.getType().akType(), column.tInstance(), column.getCollator());
    }

    public abstract <S> void append(S source, AkType type, TInstance tInstance, AkCollator collator);

    public abstract void close(boolean forInsert);

}
