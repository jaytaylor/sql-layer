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
import com.foundationdb.qp.operator.CursorLifecycle;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.RowCursorImpl;
import com.foundationdb.qp.storeadapter.Sorter;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.row.ValuesHolderRow;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.error.StorageKeySizeExceededException;
import com.foundationdb.server.types.value.ValueTargets;
import com.foundationdb.util.tap.InOutTap;
import com.persistit.Key;
import com.persistit.KeyState;
import com.persistit.exception.KeyTooLongException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * <h1>Overview</h1>
 *
 * Sort rows by inserting them into a {@link TreeMap} and then reading them out in order.
 *
 * <h1>Behavior</h1>
 *
 * The rows of the input stream are written into a map that orders rows according to the ordering specification.
 * Once the input stream has been consumed, the map is iterated from beginning to end to provide rows of the output
 * stream.
 *
 * <h1>Performance</h1>
 *
 * MemorySorter generates no IO.
 *
 * <h1>Memory Requirements</h1>
 *
 * Memory requirements are dependent on the size of the input stream. One Key is generated for each Row and each Row
 * is copied to be held in memory. All rows from the input stream are held until close.
 */
public class MemorySorter implements Sorter
{
    /*
     * Map is a set of keys to (a copy of) the original row.
     * Keys are encoded such that the output can be a single, forward iteration.
     * Each "ordering chunk" results in a single key state and comparator, with key encoding being the default
     * and the comparator inverting the result if DESC is required.
     * For example, (ASC,ASC,DESC,DESC,ASC) would result in 3 states and 3 comparators.
     */
    private final NavigableMap<KeyState[], Row> navigableMap;
    private final List<Integer> orderChanges;

    private final QueryContext context;
    private final QueryBindings bindings;
    private final RowCursor input;
    private final API.Ordering ordering;
    private final Key key;
    private final InOutTap loadTap;
    private final SorterAdapter<?, ?, ?> sorterAdapter;

    public MemorySorter(QueryContext context,
                        QueryBindings bindings,
                        RowCursor input,
                        RowType rowType,
                        API.Ordering ordering,
                        API.SortOption sortOption,
                        InOutTap loadTap,
                        Key key)
    {
        this.context = context;
        this.bindings = bindings;
        this.input = input;
        this.ordering = ordering.copy();
        this.key = key;
        this.loadTap = loadTap;
        this.sorterAdapter = new ValueSorterAdapter();
        // Note: init may change this.ordering
        sorterAdapter.init(rowType, this.ordering, this.key, null, this.context, this.bindings, sortOption);
        // Explicitly use input ordering to avoid appended field
        this.orderChanges = new ArrayList<>();
        List<Comparator<KeyState>> comparators = new ArrayList<>();
        for(int i = 0; i < ordering.sortColumns(); ++i) {
            Comparator<KeyState> c = ordering.ascending(i) ? ASC_COMPARATOR : DESC_COMPARATOR;
            if(i == 0 || ordering.ascending(i-1) != ordering.ascending(i)) {
                orderChanges.add(i);
                comparators.add(c);
            }
        }
        this.orderChanges.add(ordering.sortColumns());
        this.navigableMap = new TreeMap<>(new KeyStateArrayComparator(comparators));
    }

    @Override
    public RowCursor sort() {
        loadMap();
        return new CollectionCursor(navigableMap.values());
    }

    @Override
    public void close() {
        navigableMap.clear();
    }

    private void loadMap() {
        boolean loaded = false;
        try {
            loadTap.in();
            try {
                Row row;
                int rowCount = 0;
                while((row = input.next()) != null) {
                    ++rowCount;
                    context.checkQueryCancelation();
                    KeyState[] states = createKey(row, rowCount);
                    // Copy instead of hold as ProjectedRow cannot be held
                    ValuesHolderRow rowCopy = new ValuesHolderRow(row.rowType());
                    for(int i = 0 ; i < row.rowType().nFields(); ++i) {
                        ValueTargets.copyFrom(row.value(i), rowCopy.valueAt(i));
                    }
                    navigableMap.put(states, rowCopy);
                    loadTap.out();
                    loadTap.in();
                }
            } finally {
                loadTap.out();
            }
            loaded = true;
        } finally {
            if(!loaded) {
                close();
            }
        }
    }

    private KeyState[] createKey(Row row, int rowCount) {
        KeyState[] states = new KeyState[orderChanges.size() - 1];
        for(int i = 0; i < states.length; ++i) {
            int startOffset = orderChanges.get(i);
            int endOffset = orderChanges.get(i + 1);
            boolean isLast = i == states.length - 1;
            // Loop for key growth
            while(true) {
                try {
                    key.clear();
                    for(int j = startOffset; j < endOffset; ++j) {
                        sorterAdapter.evaluateToKey(row, j);
                    }
                    if(isLast && sorterAdapter.preserveDuplicates()) {
                        key.append(rowCount);
                    }
                    break;
                } catch (KeyTooLongException | StorageKeySizeExceededException e) {
                    key.setMaximumSize(key.getMaximumSize() * 2);
                }
            }
            states[i] = new KeyState(key);
        }
        return states;
    }

    private static final class CollectionCursor extends RowCursorImpl {
        private final Collection<Row> collection;
        private Iterator<Row> it;

        public CollectionCursor(Collection<Row> collection) {
            this.collection = collection;
        }

        @Override
        public void open() {
            super.open();
            it = collection.iterator();
        }

        @Override
        public Row next() {
            CursorLifecycle.checkIdleOrActive(this);
            if(it != null && it.hasNext()) {
                return it.next();
            }
            return null;
        }

        @Override
        public void close() {
            super.close();
            it = null;
        }
    }

    private static final Comparator<KeyState> ASC_COMPARATOR = new Comparator<KeyState>() {
        @Override
        public int compare(KeyState k1, KeyState k2) {
            return k1.compareTo(k2);
        }
    };

    private static final Comparator<KeyState> DESC_COMPARATOR = new Comparator<KeyState>() {
        @Override
        public int compare(KeyState k1, KeyState k2) {
            return k2.compareTo(k1);
        }
    };

    private static final class KeyStateArrayComparator implements Comparator<KeyState[]> {
        private final Comparator[] comparators;

        private KeyStateArrayComparator(List<Comparator<KeyState>> comparators) {
            this.comparators = comparators.toArray(new Comparator[comparators.size()]);
        }

        @SuppressWarnings("unchecked")
        @Override
        public int compare(KeyState[] k1, KeyState[] k2) {
            int val = 0;
            for(int i = 0; (i < comparators.length) && (val == 0); ++i) {
                val = comparators[i].compare(k1[i], k2[i]);
            }
            return val;
        }
    }
}
