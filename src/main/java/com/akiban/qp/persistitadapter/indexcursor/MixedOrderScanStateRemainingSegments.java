
package com.akiban.qp.persistitadapter.indexcursor;

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.exception.PersistitException;

import static com.akiban.qp.persistitadapter.indexcursor.IndexCursor.INDEX_TRAVERSE;

class MixedOrderScanStateRemainingSegments<S> extends MixedOrderScanState<S>
{
    @Override
    public boolean startScan() throws PersistitException
    {
        Exchange exchange = cursor.exchange();
        if (subtreeRootKey == null) {
            subtreeRootKey = new Key(exchange.getKey());
        } else {
            exchange.getKey().copyTo(subtreeRootKey);
        }
        INDEX_TRAVERSE.hit();
        return exchange.traverse(Key.GT, true);
    }

    @Override
    public boolean advance() throws PersistitException
    {
        INDEX_TRAVERSE.hit();
        Exchange exchange = cursor.exchange();
        boolean more = ascending ? exchange.next(true) : exchange.previous(true);
        if (more) {
            more = exchange.getKey().firstUniqueByteIndex(subtreeRootKey) >= subtreeRootKey.getEncodedSize();
        }
        if (!more) {
            // Restore exchange key to where it was before exploring this subtree. But also attach one
            // more key segment since IndexCursorMixedOrder is going to cut one.
            subtreeRootKey.copyTo(exchange.getKey());
            exchange.getKey().append(Key.BEFORE);
        }
        return more;
    }

    @Override
    public boolean jump(S fieldValue) throws PersistitException
    {
        return startScan();
    }

    public MixedOrderScanStateRemainingSegments(IndexCursorMixedOrder indexCursor, int field) throws PersistitException
    {
        super(indexCursor, field, true);
    }

    private Key subtreeRootKey;
}
