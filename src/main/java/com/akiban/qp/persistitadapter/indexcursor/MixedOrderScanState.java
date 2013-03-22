
package com.akiban.qp.persistitadapter.indexcursor;

import com.persistit.exception.PersistitException;

import static com.akiban.qp.persistitadapter.indexcursor.IndexCursor.INDEX_TRAVERSE;

abstract class MixedOrderScanState<S>
{
    public abstract boolean startScan() throws PersistitException;

    public abstract boolean jump(S fieldValue) throws PersistitException;

    public final int field()
    {
        return field;
    }

    public boolean advance() throws PersistitException
    {
        INDEX_TRAVERSE.hit();
        return ascending ? cursor.exchange().next(false) : cursor.exchange().previous(false);
    }

    protected MixedOrderScanState(IndexCursorMixedOrder cursor, int field, boolean ascending) throws PersistitException
    {
        this.cursor = cursor;
        this.field = field;
        this.ascending = ascending;
    }

    protected final IndexCursorMixedOrder cursor;
    protected final int field;
    protected final boolean ascending;
}
