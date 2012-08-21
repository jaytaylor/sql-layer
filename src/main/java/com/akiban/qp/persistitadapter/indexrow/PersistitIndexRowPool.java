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

package com.akiban.qp.persistitadapter.indexrow;

import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.rowtype.IndexRowType;

import java.util.*;

// TODO: EXPERIMENTAL

public class PersistitIndexRowPool
{
    // PersistitIndexRowPool interface

    public synchronized PersistitIndexRow takeIndexRow(PersistitAdapter adapter, IndexRowType indexRowType)
    {
        return
            poolingEnabled
            ? adapterPool(adapter).takeIndexRow(indexRowType)
            : newIndexRow(adapter, indexRowType);
    }

    public synchronized void returnIndexRow(PersistitAdapter adapter,
                                            IndexRowType indexRowType,
                                            PersistitIndexRow indexRow)
    {
        if (poolingEnabled) {
            adapterPool(adapter).returnIndexRow(indexRowType, indexRow);
        }
    }

    public synchronized void enablePooling(boolean newPoolingEnabled)
    {
        if (this.poolingEnabled && !newPoolingEnabled) {
            // Disabling
            adapterPools.clear();
        }
        this.poolingEnabled = newPoolingEnabled;
    }

    // For use by this class

    private AdapterPool adapterPool(PersistitAdapter adapter)
    {
        AdapterPool pool = adapterPools.get(adapter.id());
        if (pool == null) {
            pool = new AdapterPool(adapter);
            adapterPools.put(adapter.id(), pool);
        }
        return pool;
    }

    private static PersistitIndexRow newIndexRow(PersistitAdapter adapter, IndexRowType indexRowType)
    {
        return
            indexRowType.index().isTableIndex()
            ? new PersistitTableIndexRow(adapter, indexRowType)
            : new PersistitGroupIndexRow(adapter, indexRowType);
    }

    // Class state

    public static final String INDEX_ROW_POOLING = "akserver.indexRowPooling";
    private static final int CAPACITY = 100;
    private static final float LOAD_FACTOR = 0.7f;

    // Object state

    private boolean poolingEnabled = true;
    private final LinkedHashMap<Long, AdapterPool> adapterPools =
        new LinkedHashMap<Long, AdapterPool>(CAPACITY, LOAD_FACTOR, true /* access order for LRU */)
        {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Long, AdapterPool> eldest)
            {
                return size() > CAPACITY;
            }
        };

    // Inner classes

    private static class AdapterPool
    {
        public PersistitIndexRow takeIndexRow(IndexRowType indexRowType)
        {
            PersistitIndexRow indexRow = null;
            Deque<PersistitIndexRow> indexRows = indexRowCache.get(indexRowType);
            if (indexRows != null && !indexRows.isEmpty()) {
                indexRow = indexRows.removeLast();
                indexRow.reset();
            }
            if (indexRow == null) {
                indexRow = newIndexRow(adapter, indexRowType);
            }
            return indexRow;
        }

        public void returnIndexRow(IndexRowType indexRowType, PersistitIndexRow indexRow)
        {
            Deque<PersistitIndexRow> indexRows = indexRowCache.get(indexRowType);
            if (indexRows == null) {
                indexRows = new ArrayDeque<PersistitIndexRow>();
                indexRowCache.put(indexRowType, indexRows);
            }
            indexRows.addLast(indexRow);
        }

        public AdapterPool(PersistitAdapter adapter)
        {
            this.adapter = adapter;
        }

        private final PersistitAdapter adapter;
        private final Map<IndexRowType, Deque<PersistitIndexRow>> indexRowCache =
            new HashMap<IndexRowType, Deque<PersistitIndexRow>>();
    }
}
