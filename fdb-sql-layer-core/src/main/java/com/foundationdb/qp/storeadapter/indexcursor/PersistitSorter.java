/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.qp.storeadapter.indexcursor;

import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.RowCursor;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.storeadapter.PersistitAdapter;
import com.foundationdb.qp.storeadapter.Sorter;
import com.foundationdb.qp.storeadapter.TempVolume;
import com.foundationdb.qp.storeadapter.indexcursor.SorterAdapter.PersistitValueSourceAdapter;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.row.ValuesHolderRow;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.util.tap.InOutTap;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Key.Direction;
import com.persistit.Value;
import com.persistit.exception.KeyTooLongException;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * <h1>Overview</h1>
 *
 * Sort rows by inserting them into Persistit B-Tree and then read out in order.
 *
 * <h1>Behavior</h1>
 *
 * The rows of the input stream are written into a B-Tree that orders rows according to the ordering specification.
 * Once the input stream has been consumed, the B-Tree is traversed from beginning to end to provide rows of the output
 * stream.
 *
 * <h1>Performance</h1>
*
 * PersistitSorter generates IO dependent on the size of the input stream. This occurs mostly during the loading phase,
 * (when the input stream is being read). There will be some IO when the loaded B-Tree is scanned, but this is
 * expected to be more efficient, as each page will be read completely before moving on to the next one.
*
 * <h1>Memory Requirements</h1>
*
 * Memory requirements (and disk requirements) depend on the underlying configuration, primarily the buffer pool size,
 * and concurrent load on the system.
*/
public class PersistitSorter implements Sorter
{
    public PersistitSorter(QueryContext context,
                           QueryBindings bindings,
                           RowCursor input,
                           RowType rowType,
                           API.Ordering ordering,
                           API.SortOption sortOption,
                           InOutTap loadTap)
    {
        this.context = context;
        this.bindings = bindings;
        this.adapter = (PersistitAdapter)context.getStore();
        this.input = input;
        this.rowType = rowType;
        this.ordering = ordering.copy();
        String sortTreeName = SORT_TREE_NAME_PREFIX + SORTER_ID_GENERATOR.getAndIncrement();
        this.exchange = TempVolume.takeExchange(adapter.persistit(), adapter.getSession(), sortTreeName);
        this.key = exchange.getKey();
        this.value = exchange.getValue();
        this.rowFields = rowType.nFields();
        sorterAdapter = new ValueSorterAdapter();
        sorterAdapter.init(this.rowType, this.ordering, key, value, this.context, this.bindings, sortOption);
        iterationHelper = new SorterIterationHelper(sorterAdapter.createValueAdapter());
        this.loadTap = loadTap;

    }

    @Override
    public RowCursor sort()
    {
        loadTree();
        return cursor();
    }

    @Override
    public void close()
    {
        if (exchange != null) {
            try {
                TempVolume.returnExchange(adapter.getSession(), exchange);
            } finally {
                exchange = null;
            }
        }
    }

    private void loadTree()
    {
        boolean loaded = false;
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
            loaded = true;
        } catch (PersistitException e) {
            if (!PersistitAdapter.isFromInterruption(e))
                LOG.debug("Caught exception while loading tree for sort", e);
            adapter.handlePersistitException(e);
        } finally {
            if (!loaded) {
                close();
            }
        }
    }

    private RowCursor cursor()
    {
        exchange.clear();
        IndexCursor indexCursor = IndexCursor.create(context, null, ordering, iterationHelper, false);
        indexCursor.rebind(bindings);
        return indexCursor;
    }

    private void createKey(Row row)
    {
        while (true) {
            try {
                key.clear();
                boolean preserveDuplicates = sorterAdapter.preserveDuplicates();
                int sortFields = ordering.sortColumns() - (preserveDuplicates ? 1 : 0);
                for (int i = 0; i < sortFields; i++) {
                    sorterAdapter.evaluateToKey(row, i);
                }
                if (preserveDuplicates) {
                    key.append(rowCount++);
                }
                break;
            } catch (KeyTooLongException e) {
                key.setMaximumSize(key.getMaximumSize() * 8);
            }
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

    private static final Logger LOG = LoggerFactory.getLogger(PersistitSorter.class);
    private static final String SORT_TREE_NAME_PREFIX = "sort.";
    private static final AtomicLong SORTER_ID_GENERATOR = new AtomicLong(0);

    // Object state

    final PersistitAdapter adapter;
    final RowCursor input;
    final RowType rowType;
    final API.Ordering ordering;
    final QueryContext context;
    final QueryBindings bindings;
    final Key key;
    final Value value;
    final int rowFields;
    Exchange exchange;
    long rowCount = 0;
    private final InOutTap loadTap;
    private final SorterAdapter<?, ?, ?> sorterAdapter;
    private final IterationHelper iterationHelper;

    // Inner classes

    private class SorterIterationHelper implements IterationHelper
    {
        @Override
        public Row row()
        {
            ValuesHolderRow row = new ValuesHolderRow(rowType);
            value.setStreamMode(true);
            for (int i = 0; i < rowFields; i++) {
                valueAdapter.putToHolders(row, i, sorterAdapter.tFieldTypes());
            }
            return row;
        }

        @Override
        public void closeIteration()
        {
            PersistitSorter.this.close();
        }

        @Override
        public Key key()
        {
            return exchange.getKey();
        }

        @Override
        public void clear() {
            exchange.clear();
        }

        @Override
        public boolean next(boolean deep)
        {
            try {
                return exchange.next(deep);
            } catch(PersistitException | RollbackException e) {
                throw PersistitAdapter.wrapPersistitException(adapter.getSession(), e);
            }
        }

        @Override
        public boolean prev(boolean deep)
        {
            try {
                return exchange.previous(deep);
            } catch(PersistitException | RollbackException e) {
                throw PersistitAdapter.wrapPersistitException(adapter.getSession(), e);
            }
        }

        @Override
        public boolean traverse(Direction dir, boolean deep)
        {
            try {
                return exchange.traverse(dir, deep);
            } catch(PersistitException | RollbackException e) {
                throw PersistitAdapter.wrapPersistitException(adapter.getSession(), e);
            }
        }

        @Override
        public void openIteration()
        {
        }

        @Override
        public void preload(Direction dir, boolean deep) {
        }

        SorterIterationHelper(PersistitValueSourceAdapter valueAdapter)
        {
            this.valueAdapter = valueAdapter;
            valueAdapter.attach(value);
        }

        private final PersistitValueSourceAdapter valueAdapter;
    }
}
