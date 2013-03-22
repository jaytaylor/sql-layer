
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
