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

import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.server.types.value.ValueRecord;
import com.foundationdb.qp.expression.IndexBound;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.storeadapter.indexrow.PersistitIndexRow;
import com.foundationdb.qp.storeadapter.indexrow.PersistitIndexRowBuffer;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.server.types.TInstance;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.min;

class IndexCursorMixedOrder<S,E> extends IndexCursor
{
    // Cursor interface

    @Override
    public void open()
    {
        super.open();
        clear();
        scanStates.clear();
        boolean success = false;
        try {
            setBoundaries();
            initializeScanStates();
            repositionExchange(0);
            justOpened = true;
            pastStart = false;
            success = true;
        } finally {
            if(!success) {
                close();
            }
        }
    }

    @Override
    public Row next()
    {
        super.next();
        Row next = null;
        boolean success = false;
        try {
            if (justOpened) {
                // Exchange is already positioned
                justOpened = false;
            } else {
                advance(scanStates.size() - 1);
            }
            if (more) {
                next = row();
                // If we're scanning a unique key index, then the row format has the declared key in the
                // Persistit key, and undeclared hkey columns in the Persistit value. An index scan may actually
                // restrict the entire declared key and leading hkeys fields. If this happens, then the first
                // row found by exchange.traverse may actually not qualify -- those values may be lower than
                // startKey. This can happen at most once per scan. pastStart indicates whether we have gotten
                // past the startKey.
                if (!pastStart) {
                    while (beforeStart(next)) {
                        next = null;
                        advance(scanStates.size() - 1);
                        if (more) {
                            next = row();
                            // Guard against bug 1046053
                            assert next != startKey;
                            assert next != endKey;
                        } else {
                            close();
                        }
                    }
                    pastStart = true;
                }
                if (next != null && pastEnd(next)) {
                    next = null;
                    close();
                }
            } else {
                close();
            }
            success = true;
        } finally {
            if(!success) {
                close();
            }
        }
        return next;
    }

    @Override
    public void jump(Row row, ColumnSelector columnSelector)
    {
        // Reposition cursor by delegating jump to each MixedOrderScanState. Also recompute
        // startKey so that beforeStart() works.
        assert keyRange != null; // keyRange is null when used from a Sorter
        clear();
        boolean success = false;
        int field = 0;
        more = true;
        try {
            if (startKey != null) {
                clear(startKey);
            }
            while (more && field < scanStates.size()) {
                MixedOrderScanState<S> scanState = scanStates.get(field);
                if (columnSelector.includesColumn(field)) {
                    S fieldValue = sortKeyAdapter.eval(row, field);
                    if (!scanState.jump(fieldValue)) {
                        // We've matched as much of the row as we can with tree contents.
                        if (scanState.field() == scanStates.size() - 1) {
                            // If we're on the last key segment, then the cursor is already positioned so nothing needs
                            // to be done.
                            more = true;
                        } else {
                            more = scanState.startScan();
                        }
                    }
                    if (startKey != null) {
                        startKey.append(fieldValue, typeAt(field));
                        loBoundColumns = field + 1;
                    }
                } else {
                    more = scanState.startScan();
                }
                field++;
            }
            loInclusive = true;
            justOpened = true;
            success = true;
        } finally {
            if(!success) {
                close();
            }
        }
    }

    @Override
    public void destroy()
    {
        super.destroy();
        if (startKey != null) {
            adapter.returnIndexRow(startKey);
            startKey = null;
        }
        if (endKey != null) {
            adapter.returnIndexRow(endKey);
            endKey = null;
        }
    }

    // IndexCursorMixedOrder interface

    public static <S, E> IndexCursorMixedOrder<S, E> create(QueryContext context,
                                              IterationHelper iterationHelper,
                                              IndexKeyRange keyRange,
                                              API.Ordering ordering,
                                              SortKeyAdapter<S, E> sortKeyAdapter)
    {
        return new IndexCursorMixedOrder<>(context, iterationHelper, keyRange, ordering, sortKeyAdapter);
    }

    public void initializeScanStates()
    {
        int f = 0;
        // Use maxSegments to limit MixedOrderScanStates to just those corresponding to the columns
        // stored in a Persistit key. The off-by-one-row problems due to columns in the Persistit value
        // are dealt with in IndexCursorMixedOrder.
        int maxSegments = (keyRange == null /* sorting */) ? Integer.MAX_VALUE : index().getAllColumns().size();
        while (f < min(loBoundColumns, maxSegments)) {
            ValueRecord lo = keyRange.lo().boundExpressions(context, bindings);
            ValueRecord hi = keyRange.hi().boundExpressions(context, bindings);
            S loSource = sortKeyAdapter.get(lo, f);
            S hiSource = sortKeyAdapter.get(hi, f);
            /*
             * An index restriction is described by an IndexKeyRange which contains
             * two IndexBounds. The IndexBound wraps an index row. The fields of the row that are being restricted are
             * described by the IndexBound's ColumnSelector. The only index restrictions supported specify:
             * a) equality for zero or more fields of the index,
             * b) 0-1 inequality, and
             * c) any remaining columns unbounded.
             *
             * The key range's loInclusive and hiInclusive flags apply to b. For a, the comparisons
             * are always inclusive. E.g. if the range is >(x, y, p) and <(x, y, q), then the bounds
             * on the individual fields are (>=x, <=x), (>=y, <=y) and (>p, <q). So we want inclusive for
             * a, and whatever the key range specified for inclusivity for b. Checking the type of scan state f + 1
             * is how we distinguish cases a and b.
             *
             * The observant reader will wonder: what about >(x, y, p) and <(x, z, q)? This is a violation of condition
             * b since there are two inequalities (y != z, p != q) and it should not be possible to get this far with
             * such an IndexKeyRange.
             *
             * So for scanStates:
             * - lo(f) = hi(f), f < loBoundColumns - 1
             * - lo(f) - hi(f) defines a range, with limits described by keyRange.lo/hiInclusive,
             *   f = loBoundColumns - 1
             * The last argument to setRangeLimits determines which condition is checked.
             */
            boolean loInclusive;
            boolean hiInclusive;
            boolean singleValue;
            if (f < loBoundColumns - 1) {
                loInclusive = true;
                hiInclusive = true;
                singleValue = true;
            } else {
                loInclusive = keyRange.loInclusive();
                hiInclusive = keyRange.hiInclusive();
                singleValue = false;
            }
            MixedOrderScanStateSingleSegment<S,E> scanState =
                new MixedOrderScanStateSingleSegment<>(this,
                                                     f,
                                                     loSource,
                                                     loInclusive,
                                                     hiSource,
                                                     hiInclusive,
                                                     singleValue,
                                                     f >= orderingColumns() || ordering.ascending(f),
                                                     sortKeyAdapter);
            scanStates.add(scanState);
            f++;
        }
        while (f < min(orderingColumns(), maxSegments)) {
            MixedOrderScanStateSingleSegment<S, E> scanState =
                new MixedOrderScanStateSingleSegment<>(this, f, ordering.ascending(f), sortKeyAdapter);
            scanStates.add(scanState);
            f++;
        }
        if (f < min(keyColumns(), maxSegments)) {
            MixedOrderScanStateRemainingSegments<S> scanState =
                new MixedOrderScanStateRemainingSegments<>(this, orderingColumns());
            scanStates.add(scanState);
        }
    }

    // For use by subclasses

    protected IndexCursorMixedOrder(QueryContext context,
                                    IterationHelper iterationHelper,
                                    IndexKeyRange keyRange,
                                    API.Ordering ordering,
                                    SortKeyAdapter<S, E> sortKeyAdapter)
    {
        super(context, iterationHelper);
        this.keyRange = keyRange;
        this.ordering = ordering;
        this.ascending = new boolean[ordering.sortColumns()];
        for (int f = 0; f < orderingColumns(); f++) {
            this.ascending[f] = ordering.ascending(f);
        }
        this.sortKeyAdapter = sortKeyAdapter;
        // keyRange == null occurs when Sorter is used, (to sort an arbitrary input stream). There is no
        // IndexRowType in that case, so an IndexKeyRange can't be created.
        int orderingColumns = orderingColumns();
        if (keyRange == null) {
            keyColumns = ordering.sortColumns();
            loBoundColumns = 0;
            types = sortKeyAdapter.createTInstances(orderingColumns);
            for (int i = 0; i < orderingColumns; ++i) {
                sortKeyAdapter.setOrderingMetadata(ordering, i, types);
            }
        } else {
            Index index = keyRange.indexRowType().index();
            keyColumns = index.indexRowComposition().getLength();
            loBoundColumns = keyRange.boundColumns();
            List<IndexColumn> indexColumns = index.getAllColumns();
            int nColumns = indexColumns.size();
            types = sortKeyAdapter.createTInstances(nColumns);
            for (int f = 0; f < nColumns; f++) {
                Column column = indexColumns.get(f).getColumn();
                sortKeyAdapter.setColumnMetadata(column, f, types);
            }
            if (index.isUnique()) {
                startKey = adapter.takeIndexRow(keyRange.indexRowType());
                endKey = adapter.takeIndexRow(keyRange.indexRowType());
            }
        }
        hiBoundColumns = loBoundColumns;
    }

    // For use by this package

    API.Ordering ordering()
    {
        return ordering;
    }

    // For use by subclasses

    protected int orderingColumns()
    {
        return ordering.sortColumns();
    }

    protected int keyColumns()
    {
        return keyColumns;
    }

    // For use by this class

    private void advance(int field)
    {
        MixedOrderScanState scanState = scanStates.get(field);
        if (scanState.advance()) {
            repositionExchange(field + 1);
        } else {
            key().cut();
            if (field == 0) {
                more = false;
            } else {
                advance(field - 1);
            }
        }
    }

    private void repositionExchange(int field)
    {
        more = true;
        for (int f = field; more && f < scanStates.size(); f++) {
            more = scanStates.get(f).startScan();
        }
    }

    private Index index()
    {
        return keyRange.indexRowType().index();
    }

    private void setBoundaries()
    {
        if (keyRange != null && !loUnbounded() && index().isUnique()) {
            assert startKey != null : index();
            assert endKey != null : index();
            IndexBound lo = keyRange.lo();
            IndexBound hi = keyRange.hi();
            ValueRecord loExpressions = lo.boundExpressions(context, bindings);
            ValueRecord hiExpressions = hi.boundExpressions(context, bindings);
            int nColumns = keyRange.boundColumns();
            clear(startKey);
            clear(endKey);
            for (int f = 0; f < nColumns; f++) {
                ValueRecord startExpressions;
                ValueRecord endExpressions;
                if (ordering().ascending(f)) {
                    startExpressions = loExpressions;
                    endExpressions = hiExpressions;
                } else {
                    startExpressions = hiExpressions;
                    endExpressions = loExpressions;
                }
                startKey.append(sortKeyAdapter.get(startExpressions, f), typeAt(f));
                endKey.append(sortKeyAdapter.get(endExpressions, f), typeAt(f));
            }
            loInclusive = keyRange.loInclusive();
            hiInclusive = keyRange.hiInclusive();
        }
    }

    private void clear(PersistitIndexRowBuffer bound)
    {
        assert bound == startKey || bound == endKey;
        bound.resetForWrite(index(), adapter.createKey(), null); // TODO: Reuse the existing key
    }

    private boolean beforeStart(Row row)
    {
        boolean beforeStart;
        if (startKey == null || row == null || loUnbounded()) {
            beforeStart = false;
        } else {
            PersistitIndexRow current = (PersistitIndexRow) row;
            int c = current.compareTo(startKey, ascending);
            beforeStart = c < 0 || c == 0 && !loInclusive;
        }
        return beforeStart;
    }

    private boolean pastEnd(Row row)
    {
        boolean pastEnd;
        if (endKey == null || hiUnbounded()) {
            pastEnd = false;
        } else {
            PersistitIndexRow current = (PersistitIndexRow) row;
            int c = current.compareTo(endKey, ascending);
            pastEnd = c > 0 || c == 0 && !keyRange.hiInclusive();
        }
        return pastEnd;
    }

    private boolean loUnbounded()
    {
        return loBoundColumns == 0;
    }

    private boolean hiUnbounded()
    {
        return hiBoundColumns == 0;
    }

    public TInstance typeAt(int field) {
        return types == null ? null : types[field];
    }
    
    // Object state

    protected final IndexKeyRange keyRange;
    protected final API.Ordering ordering;
    protected final List<MixedOrderScanState<S>> scanStates = new ArrayList<>();
    private final SortKeyAdapter<S, E> sortKeyAdapter;
    private final int keyColumns; // Number of columns in the key. keyFields >= orderingColumns.
    private int loBoundColumns;
    private int hiBoundColumns;
    private boolean loInclusive;
    private boolean hiInclusive;
    private boolean more;
    private boolean justOpened;
    private TInstance[] types;
    private boolean[] ascending;
    // Used for checking first and last row in case of unique indexes. These indexes have some key state in
    // the Persistit value, and are therefore not visible to the ICMO implementation.
    private PersistitIndexRow startKey;
    private PersistitIndexRow endKey;
    private boolean pastStart;
}
