/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.qp.persistitadapter.indexcursor;

import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.CursorLifecycle;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.persistitadapter.Sorter;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.ValuesHolderRow;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.types3.pvalue.PValueTargets;
import com.akiban.util.tap.InOutTap;
import com.persistit.Key;
import com.persistit.KeyState;
import com.persistit.exception.KeyTooLongException;

import java.util.Collection;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;

public class MemorySorter implements Sorter
{
    private final NavigableMap<KeyState, Row> navigableMap;
    private final QueryContext context;
    private final Cursor input;
    private final API.Ordering ordering;
    private final Key key;
    private final InOutTap loadTap;
    private final SorterAdapter<?, ?, ?> sorterAdapter;
    private final boolean isAscending;
    private long rowCount = 0;

    public MemorySorter(QueryContext context,
                        Cursor input,
                        RowType rowType,
                        API.Ordering ordering,
                        API.SortOption sortOption,
                        InOutTap loadTap,
                        Key key)
    {
        this.navigableMap = new TreeMap<>();
        this.context = context;
        this.input = input;
        this.ordering = ordering.copy();
        this.key = key;
        this.loadTap = loadTap;
        this.sorterAdapter = new PValueSorterAdapter();
        this.isAscending = ordering.ascending(0);
        // Note: init may change this.ordering
        sorterAdapter.init(rowType, this.ordering, this.key, null, this.context, sortOption);
        for(int i = 1; i < this.ordering.sortColumns(); ++i) {
            if(isAscending != this.ordering.ascending(i)) {
                throw new UnsupportedOperationException("Mixed order sort not supported");
            }
        }
    }

    @Override
    public Cursor sort() {
        loadMap();
        return new CollectionCursor(isAscending ? navigableMap.values() : navigableMap.descendingMap().values());
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
                while((row = input.next()) != null) {
                    ++rowCount;
                    context.checkQueryCancelation();
                    createKey(row);
                    KeyState state = new KeyState(key);
                    // Copy instead of hold as ProjectedRow cannot be held
                    ValuesHolderRow rowCopy = new ValuesHolderRow(row.rowType(), true);
                    for(int i = 0 ; i < row.rowType().nFields(); ++i) {
                        PValueTargets.copyFrom(row.pvalue(i), rowCopy.pvalueAt(i));
                    }
                    navigableMap.put(state, rowCopy);
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

    private void createKey(Row row) {
        while(true) {
            try {
                key.clear();
                boolean preserveDuplicates = sorterAdapter.preserveDuplicates();
                int sortFields = ordering.sortColumns() - (preserveDuplicates ? 1 : 0);
                for(int i = 0; i < sortFields; i++) {
                    sorterAdapter.evaluateToKey(row, i);
                }
                if(preserveDuplicates) {
                    key.append(rowCount);
                }
                break;
            } catch (KeyTooLongException e) {
                key.setMaximumSize(key.getMaximumSize() * 2);
            }
        }
    }

    private static final class CollectionCursor implements Cursor {
        private final Collection<Row> collection;
        private boolean isIdle = true;
        private boolean isDestroyed = false;
        private Iterator<Row> it;

        public CollectionCursor(Collection<Row> collection) {
            this.collection = collection;
        }

        @Override
        public void open() {
            CursorLifecycle.checkIdle(this);
            it = collection.iterator();
            isIdle = false;
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
        public void jump(Row row, ColumnSelector columnSelector) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            CursorLifecycle.checkIdleOrActive(this);
            if(!isIdle) {
                it = null;
                isIdle = true;
            }
        }

        @Override
        public void destroy() {
            isDestroyed = true;
        }

        @Override
        public boolean isIdle() {
            return !isDestroyed && isIdle;
        }

        @Override
        public boolean isActive() {
            return !isDestroyed && !isIdle;
        }

        @Override
        public boolean isDestroyed() {
            return isDestroyed;
        }
    }
}
