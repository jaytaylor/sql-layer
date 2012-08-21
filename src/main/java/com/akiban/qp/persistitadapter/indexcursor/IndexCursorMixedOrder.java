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

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.qp.expression.BoundExpressions;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.persistitadapter.indexrow.PersistitIndexRow;
import com.akiban.qp.persistitadapter.indexrow.PersistitIndexRowBuffer;
import com.akiban.qp.row.Row;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.collation.AkCollator;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.TInstance;
import com.persistit.exception.PersistitException;

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
        exchange().clear();
        scanStates.clear();
        try {
            setBoundaries();
            initializeScanStates();
            repositionExchange(0);
            justOpened = true;
            pastStart = false;
        } catch (PersistitException e) {
            close();
            adapter.handlePersistitException(e);
        }
    }

    @Override
    public Row next()
    {
        super.next();
        Row next = null;
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
        } catch (PersistitException e) {
            close();
            adapter.handlePersistitException(e);
        }
        return next;
    }

    @Override
    public void jump(Row row, ColumnSelector columnSelector)
    {
        // Reposition cursor by delegating jump to each MixedOrderScanState. Also recompute
        // startKey so that beforeStart() works.
        assert keyRange != null; // keyRange is null when used from a Sorter
        exchange().clear();
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
                        startKey.append(fieldValue, akTypeAt(field), tInstanceAt(field), collatorAt(field));
                    }
                } else {
                    more = scanState.startScan();
                }
                field++;
            }
            justOpened = true;
        } catch (PersistitException e) {
            close();
            adapter.handlePersistitException(e);
        }
    }

    @Override
    public void close()
    {
        super.close();
        if (startKey != null) {
            adapter.returnIndexRow(startKey);
        }
        if (endKey != null) {
            adapter.returnIndexRow(endKey);
        }
    }

    // IndexCursorMixedOrder interface

    public static <S, E> IndexCursorMixedOrder<S, E> create(QueryContext context,
                                              IterationHelper iterationHelper,
                                              IndexKeyRange keyRange,
                                              API.Ordering ordering,
                                              SortKeyAdapter<S, E> sortKeyAdapter)
    {
        return new IndexCursorMixedOrder<S, E>(context, iterationHelper, keyRange, ordering, sortKeyAdapter);
    }

    public void initializeScanStates() throws PersistitException
    {
        int f = 0;
        // Use maxSegments to limit MixedOrderScanStates to just those corresponding to the columns
        // stored in a Persistit key. The off-by-one-row problems due to columns in the Persistit value
        // are dealt with in IndexCursorMixedOrder.
        int maxSegments =
            keyRange == null /* sorting */ ? Integer.MAX_VALUE :
            index().isUnique() ? index().getKeyColumns().size() : index().getAllColumns().size();
        while (f < min(boundColumns, maxSegments)) {
            BoundExpressions lo = keyRange.lo().boundExpressions(context);
            BoundExpressions hi = keyRange.hi().boundExpressions(context);
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
             * - lo(f) = hi(f), f < boundColumns - 1
             * - lo(f) - hi(f) defines a range, with limits described by keyRange.lo/hiInclusive,
             *   f = boundColumns - 1
             * The last argument to setRangeLimits determines which condition is checked.
             */
            boolean loInclusive;
            boolean hiInclusive;
            boolean singleValue;
            if (f < boundColumns - 1) {
                loInclusive = true;
                hiInclusive = true;
                singleValue = true;
            } else {
                loInclusive = keyRange.loInclusive();
                hiInclusive = keyRange.hiInclusive();
                singleValue = false;
            }
            MixedOrderScanStateSingleSegment<S,E> scanState =
                new MixedOrderScanStateSingleSegment<S, E> (this,
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
                new MixedOrderScanStateSingleSegment<S, E>(this, f, ordering.ascending(f), sortKeyAdapter);
            scanStates.add(scanState);
            f++;
        }
        if (keyRange != null && index().isUniqueAndMayContainNulls() && f == maxSegments) {
            // Add a segment to deal with the null separator. The ordering is that of the next segment (or ascending
            // if there is none).
            boolean ascending = f >= orderingColumns() || ordering.ascending(f);
            MixedOrderScanStateNullSeparator<S, E> scanState =
                new MixedOrderScanStateNullSeparator<S, E>(this, f, ascending, sortKeyAdapter);
            scanStates.add(scanState);
        }
        if (f < min(keyColumns(), maxSegments)) {
            MixedOrderScanStateRemainingSegments<S> scanState =
                new MixedOrderScanStateRemainingSegments<S>(this, orderingColumns());
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
            boundColumns = 0;
            tInstances = sortKeyAdapter.createTInstances(orderingColumns);
        } else {
            Index index = keyRange.indexRowType().index();
            keyColumns = index.indexRowComposition().getLength();
            boundColumns = keyRange.boundColumns();
            List<IndexColumn> indexColumns = index.getAllColumns();
            int nColumns = indexColumns.size();
            collators = sortKeyAdapter.createAkCollators(nColumns);
            akTypes = sortKeyAdapter.createAkTypes(nColumns);
            tInstances = sortKeyAdapter.createTInstances(nColumns);
            for (int f = 0; f < nColumns; f++) {
                Column column = indexColumns.get(f).getColumn();
                sortKeyAdapter.setColumnMetadata(column, f, akTypes, collators, tInstances);
            }
            if (index.isUnique()) {
                startKey = adapter.takeIndexRow(keyRange.indexRowType());
                endKey = adapter.takeIndexRow(keyRange.indexRowType());
            }
        }
        for (int i = 0; i < orderingColumns; ++i) {
            sortKeyAdapter.setOrderingMetadata(i, ordering, boundColumns, tInstances);
        }
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

    private void advance(int field) throws PersistitException
    {
        MixedOrderScanState scanState = scanStates.get(field);
        if (scanState.advance()) {
            repositionExchange(field + 1);
        } else {
            exchange().cut();
            if (field == 0) {
                more = false;
            } else {
                advance(field - 1);
            }
        }
    }

    private void repositionExchange(int field) throws PersistitException
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
        if (keyRange != null && !unbounded() && index().isUnique()) {
            assert startKey != null : index();
            assert endKey != null : index();
            IndexBound lo = keyRange.lo();
            IndexBound hi = keyRange.hi();
            BoundExpressions loExpressions = lo.boundExpressions(context);
            BoundExpressions hiExpressions = hi.boundExpressions(context);
            int nColumns = keyRange.boundColumns();
            clear(startKey);
            clear(endKey);
            for (int f = 0; f < nColumns; f++) {
                BoundExpressions startExpressions;
                BoundExpressions endExpressions;
                if (ordering().ascending(f)) {
                    startExpressions = loExpressions;
                    endExpressions = hiExpressions;
                } else {
                    startExpressions = hiExpressions;
                    endExpressions = loExpressions;
                }
                startKey.append(sortKeyAdapter.get(startExpressions, f), akTypeAt(f), tInstanceAt(f), collatorAt(f));
                endKey.append(sortKeyAdapter.get(endExpressions, f), akTypeAt(f), tInstanceAt(f), collatorAt(f));
            }
        }
    }

    private void clear(PersistitIndexRowBuffer bound)
    {
        assert bound == startKey || bound == endKey;
        bound.resetForWrite(index(), adapter.newKey(), null); // TODO: Reuse the existing key
    }

    private boolean beforeStart(Row row)
    {
        boolean beforeStart;
        if (startKey == null || row == null || unbounded()) {
            beforeStart = false;
        } else {
            PersistitIndexRow current = (PersistitIndexRow) row;
            int c = current.compareTo(startKey, ascending);
            beforeStart = c < 0 || c == 0 && !keyRange.loInclusive();
        }
        return beforeStart;
    }

    private boolean pastEnd(Row row)
    {
        boolean pastEnd;
        if (endKey == null || unbounded()) {
            pastEnd = false;
        } else {
            PersistitIndexRow current = (PersistitIndexRow) row;
            int c = current.compareTo(endKey, ascending);
            pastEnd = c > 0 || c == 0 && !keyRange.hiInclusive();
        }
        return pastEnd;
    }

    private boolean unbounded()
    {
        return boundColumns == 0;
    }

    public AkCollator collatorAt(int field) {
        return collators == null ? null : collators[field];
    }
    
    public AkType akTypeAt(int field) {
        return akTypes == null ? null : akTypes[field];
    }
    
    public TInstance tInstanceAt(int field) {
        return tInstances == null ? null : tInstances[field];
    }
    
    // Object state

    protected final IndexKeyRange keyRange;
    protected final API.Ordering ordering;
    protected final List<MixedOrderScanState<S>> scanStates = new ArrayList<MixedOrderScanState<S>>();
    private final SortKeyAdapter<S, E> sortKeyAdapter;
    private final int keyColumns; // Number of columns in the key. keyFields >= orderingColumns.
    private final int boundColumns;
    private boolean more;
    private boolean justOpened;
    private AkCollator[] collators;
    private AkType[] akTypes;
    private TInstance[] tInstances;
    private boolean[] ascending;
    // Used for checking first and last row in case of unique indexes. These indexes have some key state in
    // the Persistit value, and are therefore not visible to the ICMO implementation.
    private PersistitIndexRow startKey;
    private PersistitIndexRow endKey;
    private boolean pastStart;
}
