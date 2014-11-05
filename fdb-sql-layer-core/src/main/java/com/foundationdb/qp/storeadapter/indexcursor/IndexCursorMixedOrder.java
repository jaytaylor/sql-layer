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
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.CursorLifecycle;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.row.IndexRow;
import com.foundationdb.qp.row.IndexRow.EdgeValue;
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
                setIdle();
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
                // Two cases where traverse would return a row outside of bound:
                // 1) Index row that contains columns in both key and value (traverse only handles key)
                // 2) jump() put us outside of bound after traversing
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
                            setIdle();
                        }
                    }
                    pastStart = true;
                }
                if (next != null && pastEnd(next)) {
                    next = null;
                    setIdle();
                }
            } else {
                setIdle();
            }
            success = true;
        } finally {
            if(!success) {
                setIdle();
            }
        }
        return next;
    }

    @Override
    public void jump(Row row, ColumnSelector columnSelector)
    {
        assert keyRange != null; // keyRange is null when used from a Sorter
        CursorLifecycle.checkIdleOrActive(this);
        state = CursorLifecycle.CursorState.ACTIVE;
        // Reposition cursor by delegating jump to each MixedOrderScanState. Also recompute
        // startKey so that beforeStart() works.
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
                        startBoundColumns = field + 1;
                    }
                } else {
                    more = scanState.startScan();
                }
                field++;
            }
            startInclusive = true;
            justOpened = true;
            success = true;
        } finally {
            if(!success) {
                setIdle();
            }
        }
    }

    @Override
    public void close()
    {
        super.close();
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
        int maxSegments = (keyRange == null /* sorting */) ? Integer.MAX_VALUE : index().getAllColumns().size();
        while (f < min(startBoundColumns, maxSegments)) {
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
             * a, and whatever the key range specified for inclusiveness for b. Checking the type of scan state f + 1
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
            if (f < startBoundColumns - 1) {
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
                                                     ascending[f],
                                                     sortKeyAdapter);
            scanStates.add(scanState);
            f++;
        }
        while (f < min(orderingColumns(), maxSegments)) {
            MixedOrderScanStateSingleSegment<S, E> scanState =
                new MixedOrderScanStateSingleSegment<>(this, f, ascending[f], sortKeyAdapter);
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
        int keyRangeSize = (keyRange != null) ? keyRange.indexRowType().nFields() : 0;
        int orderingSize = ordering.sortColumns();
        this.ascending = new boolean[Math.max(keyRangeSize, orderingSize)];
        for (int f = 0; f < ascending.length; f++) {
            if(f >= orderingSize) {
                this.ascending[f] = index().getAllColumns().get(f).isAscending();
            } else {
                this.ascending[f] = ordering.ascending(f);
            }
        }
        this.sortKeyAdapter = sortKeyAdapter;
        // keyRange == null occurs when Sorter is used, (to sort an arbitrary input stream). There is no
        // IndexRowType in that case, so an IndexKeyRange can't be created.
        if (keyRange == null) {
            int orderingColumns = orderingColumns();
            keyColumns = ordering.sortColumns();
            startBoundColumns = 0;
            types = sortKeyAdapter.createTInstances(orderingColumns);
            for (int i = 0; i < orderingColumns; ++i) {
                sortKeyAdapter.setOrderingMetadata(ordering, i, types);
            }
        } else {
            Index index = keyRange.indexRowType().index();
            keyColumns = index.indexRowComposition().getLength();
            startBoundColumns = keyRange.boundColumns();
            List<IndexColumn> indexColumns = index.getAllColumns();
            int nColumns = indexColumns.size();
            types = sortKeyAdapter.createTInstances(nColumns);
            for (int f = 0; f < nColumns; f++) {
                Column column = indexColumns.get(f).getColumn();
                sortKeyAdapter.setColumnMetadata(column, f, types);
            }
            startKey = adapter.takeIndexRow(keyRange.indexRowType());
            endKey = adapter.takeIndexRow(keyRange.indexRowType());
        }
        endBoundColumns = startBoundColumns;
        assert keyColumns >= ordering.sortColumns();
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
        MixedOrderScanState<S> scanState = scanStates.get(field);
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
        if((keyRange != null) && (keyRange.boundColumns() > 0)) {
            assert startBoundColumns == endBoundColumns;

            clear(startKey);
            clear(endKey);

            ValueRecord startExpressions = keyRange.lo().boundExpressions(context, bindings);
            ValueRecord endExpressions = keyRange.hi().boundExpressions(context, bindings);
            int f = 0;
            while (f < keyRange.boundColumns() - 1) {
                sortKeyAdapter.checkConstraints(startExpressions, endExpressions, f, null, types);
                startKey.append(sortKeyAdapter.get(startExpressions, f), typeAt(f));
                endKey.append(sortKeyAdapter.get(endExpressions, f), typeAt(f));
                f++;
            }

            startInclusive = keyRange.loInclusive();
            endInclusive = keyRange.hiInclusive();

            // See IndexCursorUnidirectional#evaluateBoundaries() for case numbers

            // Start value
            S sVal = sortKeyAdapter.get(startExpressions, f);
            startKey.append(sVal, typeAt(f));
            // End value
            S eVal = sortKeyAdapter.get(endExpressions, f);
            if(endInclusive) {
                if(sortKeyAdapter.isNull(eVal) && (!startInclusive || !sortKeyAdapter.isNull(sVal))) {
                    // 2, 6, 14
                    throw new IllegalArgumentException();
                }
                // 3, 7, 10, 11, 15
                endKey.append(eVal, typeAt(f));
            } else {
                if(sortKeyAdapter.isNull(eVal)) {
                    // 0, 4, 8, 12
                    endKey.append(EdgeValue.AFTER);
                } else {
                    // 1, 5, 9, 13
                    endKey.append(eVal, typeAt(f));
                }
            }

            // Swap for DESC (only last column matters as they must match until then)
            if(!ascending[f]) {
                startInclusive = keyRange.hiInclusive();
                endInclusive = keyRange.loInclusive();
                IndexRow tmpKey = startKey;
                startKey = endKey;
                endKey = tmpKey;
            }
        }
    }

    private void clear(IndexRow bound)
    {
        assert bound == startKey || bound == endKey;
        bound.resetForWrite(index(), adapter.getKeyCreator().createKey()); // TODO: Reuse the existing key
    }

    private boolean beforeStart(Row row)
    {
        boolean beforeStart;
        if (startKey == null || row == null || startUnbounded()) {
            beforeStart = false;
        } else {
            IndexRow current = (IndexRow) row;
            int c = current.compareTo(startKey, startBoundColumns, ascending);
            beforeStart = c < 0 || c == 0 && !startInclusive;
        }
        return beforeStart;
    }

    private boolean pastEnd(Row row)
    {
        boolean pastEnd;
        if (endKey == null || endUnbounded()) {
            pastEnd = false;
        } else {
            IndexRow current = (IndexRow) row;
            int c = current.compareTo(endKey, endBoundColumns, ascending);
            pastEnd = c > 0 || c == 0 && !endInclusive;
        }
        return pastEnd;
    }

    private boolean startUnbounded()
    {
        return startBoundColumns == 0;
    }

    private boolean endUnbounded()
    {
        return endBoundColumns == 0;
    }

    public TInstance typeAt(int field) {
        return types == null ? null : types[field];
    }
    
    // Object state

    protected final IndexKeyRange keyRange;
    protected final API.Ordering ordering;
    protected final List<MixedOrderScanState<S>> scanStates = new ArrayList<>();
    private final SortKeyAdapter<S, E> sortKeyAdapter;
    // Number of columns in the key. keyFields >= orderingColumns.
    private final int keyColumns;
    private int startBoundColumns;
    private int endBoundColumns;
    private boolean startInclusive;
    private boolean endInclusive;
    private boolean more;
    private boolean justOpened;
    private TInstance[] types;
    // Entry for every column in the index (larger than ordering.sortColumns() if under-specified)
    private boolean[] ascending;
    private IndexRow startKey;
    private IndexRow endKey;
    private boolean pastStart;
}
