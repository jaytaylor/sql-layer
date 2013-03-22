
package com.akiban.qp.persistitadapter.indexcursor;


import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.exception.PersistitException;

class MixedOrderScanStateNullSeparator<S,E> extends MixedOrderScanStateSingleSegment<S, E>
{
    @Override
    public boolean jump(S fieldValue) throws PersistitException
    {
        Exchange exchange = cursor.exchange();
        if (!ascending) {
            exchange.append(Key.AFTER);
        }
        boolean resume = exchange.traverse(ascending ? Key.Direction.GTEQ : Key.Direction.LTEQ, true);
        return resume;
    }

    public MixedOrderScanStateNullSeparator(IndexCursorMixedOrder cursor,
                                            int field,
                                            boolean ascending,
                                            SortKeyAdapter<S, E> sortKeyAdapter)
        throws PersistitException
    {
        super(cursor, field, ascending, sortKeyAdapter, MNumeric.BIGINT.instance(false));
    }
}
