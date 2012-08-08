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
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.CursorLifecycle;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.persistitadapter.indexrow.PersistitIndexRow;
import com.akiban.qp.row.Row;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.collation.AkCollator;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.TInstance;
import com.akiban.util.tap.PointTap;
import com.akiban.util.tap.Tap;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.exception.PersistitException;

import java.util.List;

public abstract class IndexCursor<S> implements Cursor
{
    // Cursor interface

    @Override
    public void open()
    {
        CursorLifecycle.checkIdle(this);
        idle = false;
    }

    @Override
    public Row next()
    {
        CursorLifecycle.checkIdleOrActive(this);
        return null;
    }

    @Override
    public void jump(Row row, ColumnSelector columnSelector)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public final void close()
    {
        CursorLifecycle.checkIdleOrActive(this);
        iterationHelper.close();
        idle = true;
    }

    @Override
    public void destroy()
    {
        destroyed = true;
    }

    @Override
    public boolean isIdle()
    {
        return !destroyed && idle;
    }

    @Override
    public boolean isActive()
    {
        return !destroyed && !idle;
    }

    @Override
    public boolean isDestroyed()
    {
        return destroyed;
    }

    // IndexCursor interface

    public static IndexCursor create(QueryContext context,
                                     IndexKeyRange keyRange,
                                     API.Ordering ordering,
                                     IterationHelper iterationHelper,
                                     boolean usePValues)
    {
        SortKeyAdapter<?, ?> adapter = usePValues
                ? PValueSortKeyAdapter.INSTANCE
                : OldExpressionsSortKeyAdapter.INSTANCE;
        return
            ordering.allAscending() || ordering.allDescending()
            ? (keyRange != null && keyRange.lexicographic()
               ? IndexCursorUnidirectionalLexicographic.create(context, iterationHelper, keyRange, ordering, adapter)
               : IndexCursorUnidirectional.create(context, iterationHelper, keyRange, ordering, adapter))
            : IndexCursorMixedOrder.create(context, iterationHelper, keyRange, ordering, adapter);
    }

    // For use by subclasses

    protected void initializeCursor(IndexKeyRange keyRange, API.Ordering ordering)
    {
        this.keyRange = keyRange;
        this.startBoundColumns = keyRange.boundColumns();
        this.ordering = ordering;
        if (ordering.allAscending()) {
            this.direction = FORWARD;
            this.start = keyRange.lo();
            this.startInclusive = keyRange.loInclusive();
            this.end = keyRange.hi();
            this.endInclusive = keyRange.hiInclusive();
            this.keyComparison = startInclusive ? Key.GTEQ : Key.GT;
            this.subsequentKeyComparison = Key.GT;
            this.startBoundary = Key.BEFORE;
        } else if (ordering.allDescending()) {
            this.direction = BACKWARD;
            this.start = keyRange.hi();
            this.startInclusive = keyRange.hiInclusive();
            this.end = keyRange.lo();
            this.endInclusive = keyRange.loInclusive();
            this.keyComparison = startInclusive ? Key.LTEQ : Key.LT;
            this.subsequentKeyComparison = Key.LT;
            this.startBoundary = Key.AFTER;
        } else {
            assert false : ordering;
        }
        this.startKey = PersistitIndexRow.newIndexRow(adapter, keyRange.indexRowType());
    }

    protected void evaluateBoundaries(QueryContext context, SortKeyAdapter<S, ?> keyAdapter)
    {
        if (startKey != null) {
            if (startBoundColumns == 0) {
                startKey.append(startBoundary);
            } else {
                // Check constraints on start and end
                BoundExpressions loExpressions = keyRange.lo().boundExpressions(context);
                BoundExpressions hiExpressions = keyRange.hi().boundExpressions(context);
                for (int f = 0; f < endBoundColumns - 1; f++) {
                    keyAdapter.checkConstraints(loExpressions, hiExpressions, f, collators, tInstances);
                }
                /*
                    Null bounds are slightly tricky. An index restriction is described by an IndexKeyRange which contains
                    two IndexBounds. The IndexBound wraps an index row. The fields of the row that are being restricted are
                    described by the IndexBound's ColumnSelector. The only index restrictions supported specify:
                    a) equality for zero or more fields of the index,
                    b) 0-1 inequality, and
                    c) any remaining columns unbounded.

                    By the time we get here, we've stopped paying attention to part c. Parts a and b occupy the first
                    orderingColumns columns of the index. Now about the nulls: For each field of parts a and b, we have a
                    lo value and a hi value. There are four cases:

                    - both lo and hi are non-null: Just write the field values into startKey and endKey.

                    - lo is null: Write null into the startKey.

                    - hi is null, lo is not null: This restriction says that we want everything to the right of
                      the lo value. Persistit ranks null lower than anything, so instead of writing null to endKey,
                      we write Key.AFTER.

                    - lo and hi are both null: This is NOT an unbounded case. This means that we are restricting both
                      lo and hi to be null, so write null, not Key.AFTER to endKey.
                */
                // Construct start and end keys
                BoundExpressions startExpressions = start.boundExpressions(context);
                BoundExpressions endExpressions = end.boundExpressions(context);
                // startBoundColumns == endBoundColumns because jump() hasn't been called.
                // If it had we'd be in reevaluateBoundaries, not here.
                assert startBoundColumns == endBoundColumns;
                S[] startValues = keyAdapter.createSourceArray(startBoundColumns);
                S[] endValues = keyAdapter.createSourceArray(endBoundColumns);
                for (int f = 0; f < startBoundColumns; f++) {
                    startValues[f] = keyAdapter.get(startExpressions, f);
                    endValues[f] = keyAdapter.get(endExpressions, f);
                }
                clear(startKey);
                clear(endKey);
                // Construct bounds of search. For first boundColumns - 1 columns, if start and end are both null,
                // interpret the nulls literally.
                int f = 0;
                while (f < startBoundColumns - 1) {
                    startKey.append(startValues[f], type(f), tInstance(f), collator(f));
                    endKey.append(startValues[f], type(f), tInstance(f), collator(f));
                    f++;
                }
                // For the last column:
                //  0   >   null      <   null:      (null, AFTER)
                //  1   >   null      <   non-null:  (null, end)
                //  2   >   null      <=  null:      Shouldn't happen
                //  3   >   null      <=  non-null:  (null, end]
                //  4   >   non-null  <   null:      (start, AFTER)
                //  5   >   non-null  <   non-null:  (start, end)
                //  6   >   non-null  <=  null:      Shouldn't happen
                //  7   >   non-null  <=  non-null:  (start, end]
                //  8   >=  null      <   null:      [null, AFTER)
                //  9   >=  null      <   non-null:  [null, end)
                // 10   >=  null      <=  null:      [null, null]
                // 11   >=  null      <=  non-null:  [null, end]
                // 12   >=  non-null  <   null:      [start, AFTER)
                // 13   >=  non-null  <   non-null:  [start, end)
                // 14   >=  non-null  <=  null:      Shouldn't happen
                // 15   >=  non-null  <=  non-null:  [start, end]
                //
                if (direction == FORWARD) {
                    // Start values
                    startKey.append(startValues[f], type(f), tInstance(f), collator(f));
                    // End values
                    if (keyAdapter.isNull(endValues[f])) {
                        if (endInclusive) {
                            if (startInclusive && keyAdapter.isNull(startValues[f])) {
                                // Case 10:
                                endKey.append(endValues[f], type(f), tInstance(f), collator(f));
                            } else {
                                // Cases 2, 6, 14:
                                throw new IllegalArgumentException();
                            }
                        } else {
                            // Cases 0, 4, 8, 12
                            endKey.append(Key.AFTER);
                        }
                    } else {
                        // Cases 1, 3, 5, 7, 9, 11, 13, 15
                        endKey.append(endValues[f], type(f), tInstance(f), collator(f));
                    }
                } else {
                    // Same as above, swapping start and end
                    // End values
                    endKey.append(endValues[f], type(f), tInstance(f), collator(f));
                    // Start values
                    if (keyAdapter.isNull(startValues[f])) {
                        if (startInclusive) {
                            if (endInclusive && keyAdapter.isNull(endValues[f])) {
                                // Case 10:
                                startKey.append(startValues[f], type(f), tInstance(f), collator(f));
                            } else {
                                // Cases 2, 6, 14:
                                throw new IllegalArgumentException();
                            }
                        } else {
                            // Cases 0, 4, 8, 12
                            startKey.append(Key.AFTER);
                        }
                    } else {
                        // Cases 1, 3, 5, 7, 9, 11, 13, 15
                        startKey.append(startValues[f], type(f), tInstance(f), collator(f));
                    }
                }
            }
        }
    }

    // A lot like evaluateBoundaries, but simplified because end state can be left alone.
    protected void reevaluateBoundaries(QueryContext context, SortKeyAdapter<S, ?> keyAdapter)
    {
        if (startBoundColumns == 0) {
            startKey.append(startBoundary);
        } else {
            // Construct start key
            BoundExpressions startExpressions = start.boundExpressions(context);
            S[] startValues = keyAdapter.createSourceArray(startBoundColumns);
            for (int f = 0; f < startBoundColumns; f++) {
                startValues[f] = keyAdapter.get(startExpressions, f);
            }
            clear(startKey);
            // Construct bounds of search. For first boundColumns - 1 columns, if start and end are both null,
            // interpret the nulls literally.
            int f = 0;
            while (f < startBoundColumns - 1) {
                startKey.append(startValues[f], type(f), tInstance(f), collator(f));
                f++;
            }
            if (direction == FORWARD) {
                startKey.append(startValues[f], type(f), tInstance(f), collator(f));
            } else {
                if (keyAdapter.isNull(startValues[f])) {
                    if (startInclusive) {
                        // Assume case 10, the only valid choice here. On evaluateBoundaries, cases 2, 6, 14
                        // would have thrown IllegalArgumentException.
                        startKey.append(startValues[f], type(f), tInstance(f), collator(f));
                    } else {
                        // Cases 0, 4, 8, 12
                        startKey.append(Key.AFTER);
                    }
                } else {
                    // Cases 1, 3, 5, 7, 9, 11, 13, 15
                    startKey.append(startValues[f], type(f), tInstance(f), collator(f));
                }
            }
        }
    }

    protected void clear(PersistitIndexRow bound)
    {
        assert bound == startKey || bound == endKey;
        bound.resetForWrite(index(), adapter.newKey(), null); // TODO: Reuse the existing key
    }

    protected Index index()
    {
        return keyRange.indexRowType().index();
    }

    protected Row row() throws PersistitException
    {
        return iterationHelper.row();
    }

    protected AkType type(int f)
    {
        return types == null ? null : types[f];
    }

    protected TInstance tInstance(int f)
    {
        return tInstances == null ? null : tInstances[f];
    }

    protected AkCollator collator(int f)
    {
        return collators == null ? null : collators[f];
    }

    protected IndexCursor(QueryContext context,
                          IterationHelper iterationHelper,
                          SortKeyAdapter<S, ?> sortKeyAdapter,
                          IndexKeyRange keyRange)
    {
        this.context = context;
        this.adapter = (PersistitAdapter)context.getStore();
        this.iterationHelper = iterationHelper;
        this.exchange = iterationHelper.exchange();
        this.sortKeyAdapter = sortKeyAdapter;
        this.keyRange = keyRange;
        List<IndexColumn> indexColumns = keyRange.indexRowType().index().getAllColumns();
        int nColumns = indexColumns.size();
        this.types = sortKeyAdapter.createAkTypes(nColumns);
        this.collators = sortKeyAdapter.createAkCollators(nColumns);
        this.tInstances = sortKeyAdapter.createTInstances(nColumns);
        for (int f = 0; f < nColumns; f++) {
            Column column = indexColumns.get(f).getColumn();
            sortKeyAdapter.setColumnMetadata(column, f, types, collators, tInstances);
        }
    }

    // Class state

    static final PointTap SORT_TRAVERSE = Tap.createCount("traverse_sort");
    protected static final int FORWARD = 1;
    protected static final int BACKWARD = -1;

    // Object state

    protected final QueryContext context;
    protected final PersistitAdapter adapter;
    protected final Exchange exchange;
    protected final IterationHelper iterationHelper;
    protected IndexKeyRange keyRange;
    protected API.Ordering ordering;
    protected IndexBound start;
    protected IndexBound end;
    // start/endBoundColumns is the number of index fields with restrictions. They start out having the same value.
    // But jump(Row) resets state pertaining to the start of a scan, including startBoundColumns.
    protected int startBoundColumns;
    protected int endBoundColumns;
    protected boolean startInclusive;
    protected boolean endInclusive;
    protected PersistitIndexRow startKey;
    protected PersistitIndexRow endKey;
    protected SortKeyAdapter<S, ?> sortKeyAdapter;
    protected AkType[] types;
    protected TInstance[] tInstances;
    protected AkCollator[] collators;
    protected Key.Direction keyComparison;
    protected Key.Direction subsequentKeyComparison;
    protected Key.EdgeValue startBoundary; // Start of a scan that is unbounded at the start
    protected int direction; // +1 = ascending, -1 = descending
    private boolean idle = true;
    private boolean destroyed = false;
}
