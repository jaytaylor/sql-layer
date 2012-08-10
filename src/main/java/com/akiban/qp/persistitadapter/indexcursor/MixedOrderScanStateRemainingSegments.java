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

package com.akiban.qp.persistitadapter.indexcursor;

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.exception.PersistitException;

import static com.akiban.qp.persistitadapter.indexcursor.IndexCursor.SORT_TRAVERSE;

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
        SORT_TRAVERSE.hit();
        return exchange.traverse(Key.GT, true);
    }

    @Override
    public boolean advance() throws PersistitException
    {
        SORT_TRAVERSE.hit();
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
