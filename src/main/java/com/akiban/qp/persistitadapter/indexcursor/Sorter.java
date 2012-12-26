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

import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.persistitadapter.TempVolume;
import com.akiban.qp.persistitadapter.indexcursor.SorterAdapter.PersistitValueSourceAdapter;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.ValuesHolderRow;
import com.akiban.qp.rowtype.RowType;
import com.akiban.util.tap.InOutTap;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Value;
import com.persistit.exception.PersistitException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class Sorter
{
    public Sorter(QueryContext context,
                  Cursor input, 
                  RowType rowType, 
                  API.Ordering ordering,
                  API.SortOption sortOption,
                  InOutTap loadTap,
                  boolean usePValues)
        throws PersistitException
    {
        this.context = context;
        this.adapter = (PersistitAdapter)context.getStore();
        this.input = input;
        this.rowType = rowType;
        this.ordering = ordering.copy();
        String sortTreeName = SORT_TREE_NAME_PREFIX + SORTER_ID_GENERATOR.getAndIncrement();
        this.exchange = TempVolume.takeExchange(adapter, sortTreeName);
        this.key = exchange.getKey();
        this.value = exchange.getValue();
        this.rowFields = rowType.nFields();
        sorterAdapter = usePValues
                ? new PValueSorterAdapter()
                : new OldSorterAdapter();
        sorterAdapter.init(this.rowType, this.ordering, key, value, this.context, sortOption);
        iterationHelper = new SorterIterationHelper(sorterAdapter.createValueAdapter());
        this.loadTap = loadTap;
        this.usePValues = usePValues;
    }

    public Cursor sort() throws PersistitException
    {
        loadTree();
        return cursor();
    }

    public void close()
    {
        if (exchange != null) {
            try {
                TempVolume.returnExchange(adapter, exchange);
            } finally {
                // Don't return the exchange. TreeServiceImpl caches it for the tree, and we're done with the tree.
                // THIS CAUSES A LEAK OF EXCHANGES: adapter.returnExchange(exchange);
                exchange = null;
            }
        }
    }

    private void loadTree() throws PersistitException
    {
        try {
            loadTap.in();
            try {
                Row row = input.next();
                while (row != null) {
                    context.checkQueryCancelation();
                    createKey(row);
                    createValue(row);
                    exchange.store();
                    loadTap.out();
                    loadTap.in();
                    row = input.next();
                }
            } finally {
                loadTap.out();
            }
        } catch (PersistitException e) {
            if (!PersistitAdapter.isFromInterruption(e))
                LOG.error("Caught exception while loading tree for sort", e);
            exchange.removeAll();
            adapter.handlePersistitException(e);
        }
    }

    private Cursor cursor()
    {
        exchange.clear();
        return IndexCursor.create(context, null, ordering, iterationHelper, usePValues);
    }

    private void createKey(Row row)
    {
        key.clear();
        boolean preserveDuplicates = sorterAdapter.preserveDuplicates();
        int sortFields = ordering.sortColumns() - (preserveDuplicates ? 1 : 0);
        for (int i = 0; i < sortFields; i++) {
            sorterAdapter.evaluateToKey(row, i);
        }
        if (preserveDuplicates) {
            key.append(rowCount++);
        }
    }

    private void createValue(Row row)
    {
        value.clear();
        value.setStreamMode(true);
        for (int i = 0; i < rowFields; i++) {
            sorterAdapter.evaluateToTarget(row, i);
        }
    }

    // Class state

    private static final Logger LOG = LoggerFactory.getLogger(Sorter.class);
    private static final String SORT_TREE_NAME_PREFIX = "sort.";
    private static final AtomicLong SORTER_ID_GENERATOR = new AtomicLong(0);

    // Object state

    final PersistitAdapter adapter;
    final Cursor input;
    final RowType rowType;
    final API.Ordering ordering;
    final QueryContext context;
    final Key key;
    final Value value;
    final int rowFields;
    private final boolean usePValues;
    Exchange exchange;
    long rowCount = 0;
    private final InOutTap loadTap;
    private final SorterAdapter<?,?,?> sorterAdapter;
    private final IterationHelper iterationHelper;

    // Inner classes

    private class SorterIterationHelper implements IterationHelper
    {
        @Override
        public Row row()
        {
            ValuesHolderRow row = new ValuesHolderRow(rowType, usePValues);
            value.setStreamMode(true);
            for (int i = 0; i < rowFields; i++) {
                valueAdapter.putToHolders(row, i, sorterAdapter.oFieldTypes());
            }
            return row;
        }

        @Override
        public void closeIteration()
        {
            Sorter.this.close();
        }

        @Override
        public void openIteration()
        {
        }

        @Override
        public Exchange exchange()
        {
            return exchange;
        }

        SorterIterationHelper(PersistitValueSourceAdapter valueAdapter)
        {
            this.valueAdapter = valueAdapter;
            valueAdapter.attach(value);
        }

        private final PersistitValueSourceAdapter valueAdapter;
    }
}
