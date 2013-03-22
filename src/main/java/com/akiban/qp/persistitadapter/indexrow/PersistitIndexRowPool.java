
package com.akiban.qp.persistitadapter.indexrow;

import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.rowtype.IndexRowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class PersistitIndexRowPool
{
    // PersistitIndexRowPool interface

    public PersistitIndexRow takeIndexRow(PersistitAdapter adapter, IndexRowType indexRowType)
    {
        PersistitIndexRow persistitIndexRow = adapterPool(adapter).takeIndexRow(indexRowType);
        // new RuntimeException("take " + persistitIndexRow.hashCode()).printStackTrace();
        return persistitIndexRow;
    }

    public void returnIndexRow(PersistitAdapter adapter, IndexRowType indexRowType, PersistitIndexRow indexRow)
    {
        // new RuntimeException("return " + indexRow.hashCode()).printStackTrace();
        adapterPool(adapter).returnIndexRow(indexRowType, indexRow);
    }

    public PersistitIndexRowPool()
    {
    }

    // For use by this class

    private AdapterPool adapterPool(PersistitAdapter adapter)
    {
        LinkedHashMap<Long, AdapterPool> adapterPool = threadAdapterPools.get();
        AdapterPool pool = adapterPool.get(adapter.id());
        if (pool == null) {
            pool = new AdapterPool(adapter);
            adapterPool.put(adapter.id(), pool);
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

    private static final int CAPACITY_PER_THREAD = 10;
    private static final float LOAD_FACTOR = 0.7f;

    // Object state

    private final ThreadLocal<LinkedHashMap<Long, AdapterPool>> threadAdapterPools =
        new ThreadLocal<LinkedHashMap<Long, AdapterPool>>()
        {
            @Override
            protected LinkedHashMap<Long, AdapterPool> initialValue()
            {
                return new LinkedHashMap<Long, AdapterPool>(CAPACITY_PER_THREAD, LOAD_FACTOR, true /* access order for LRU */)
                {
                    @Override
                    protected boolean removeEldestEntry(Map.Entry<Long, AdapterPool> eldest)
                    {
                        return size() > CAPACITY_PER_THREAD;
                    }
                };
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
                indexRows = new ArrayDeque<>();
                indexRowCache.put(indexRowType, indexRows);
            }
            assert !indexRows.contains(indexRow);
            indexRows.addLast(indexRow);
        }

        public AdapterPool(PersistitAdapter adapter)
        {
            this.adapter = adapter;
        }

        private final PersistitAdapter adapter;
        private final Map<IndexRowType, Deque<PersistitIndexRow>> indexRowCache =
            new HashMap<>();
    }
}
