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
import com.foundationdb.qp.operator.CursorLifecycle;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.rowtype.InternalIndexTypes;
import com.foundationdb.qp.storeadapter.SpatialHelper;
import com.foundationdb.qp.row.IndexRow;
import com.foundationdb.qp.row.IndexRow.EdgeValue;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.server.types.TInstance;
import com.persistit.Key;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IndexCursorUnidirectional<S> extends IndexCursor
{
    // Cursor interface

    @Override
    public void open()
    {
        super.open();
        keyRange = initialKeyRange;
        if (keyRange != null) {
            initializeCursor();
            // end state never changes while cursor is open
            // start state can change on a jump, so it is set in initializeCursor.
            this.endBoundColumns = keyRange.boundColumns();
            this.endKey = endBoundColumns == 0 ? null : adapter.takeIndexRow(keyRange.indexRowType());
        }
        evaluateBoundaries(context, sortKeyAdapter);
        initializeForOpen();
    }

    @Override
    public Row next()
    {
        super.next();
        Row next = null;
        
        if (isIdle()) {
            return next;
        }
        
        boolean success = false;
        try {
            INDEX_TRAVERSE.hit();
            if (traverse(keyComparison)) {
                next = row();
                // Guard against bug 1046053
                assert next != startKey;
                assert next != endKey;
                // Two cases where traverse would return a row outside of bound:
                // 1) Index row that contains columns in both key and value (traverse only handles key)
                // 2) jump() put us outside of bound after traversing
                if (!pastStart) {
                    while (beforeStart(next)) {
                        next = null;
                        if (traverse(subsequentKeyComparison)) {
                            next = row();
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
        keyComparison = subsequentKeyComparison;
        return next;
    }

    
    @Override
    public void setIdle() {
        super.setIdle();
        if (startKey != null) {
            clearStart();
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

    @Override
    public void jump(Row row, ColumnSelector columnSelector)
    {
        assert keyRange != null;
        CursorLifecycle.checkIdleOrActive(this);
        // if the previous uses of the cursor have moved off the end (isIdle()) 
        // reset to active state for processing
        if (isIdle()) { setActive(); }
        keyRange =
            direction == FORWARD
            ? keyRange.resetLo(new IndexBound(row, columnSelector))
            : keyRange.resetHi(new IndexBound(row, columnSelector));
        initializeCursor();
        reevaluateBoundaries(context, sortKeyAdapter);
        initializeForOpen();
    }

    // IndexCursorUnidirectional interface

    public static <S> IndexCursorUnidirectional<S> create(QueryContext context,
                                                  IterationHelper iterationHelper,
                                                  IndexKeyRange keyRange,
                                                  API.Ordering ordering,
                                                  SortKeyAdapter<S, ?> sortKeyAdapter)
    {
        return
            keyRange == null // occurs if we're doing a sort (PersistitSorter)
            ? new IndexCursorUnidirectional<>(context, iterationHelper, ordering, sortKeyAdapter)
            : new IndexCursorUnidirectional<>(context, iterationHelper, keyRange, ordering, sortKeyAdapter);
    }

    // For use by this subclasses

    protected IndexCursorUnidirectional(QueryContext context,
                                        IterationHelper iterationHelper,
                                        IndexKeyRange keyRange,
                                        API.Ordering ordering,
                                        SortKeyAdapter<S, ?> sortKeyAdapter)
    {
        super(context, iterationHelper);
        this.initialKeyRange = keyRange;
        this.ordering = ordering;
        this.sortKeyAdapter = sortKeyAdapter;
    }

    protected void evaluateBoundaries(QueryContext context, SortKeyAdapter<S, ?> keyAdapter)
    {
        if (startKey != null) {
            if (startBoundColumns == 0) {
                startKey.append(startBoundary);
            } else {
                // Check constraints on start and end
                // Construct start and end keys
                
                ValueRecord startExpressions = start.boundExpressions(context, bindings);
                ValueRecord endExpressions = end.boundExpressions(context, bindings);
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
                // startBoundColumns == endBoundColumns because jump() hasn't been called.
                // If it had we'd be in reevaluateBoundaries, not here.
                assert startBoundColumns == endBoundColumns;
                
                for (int f = 0; f < startBoundColumns - 1; f++) {
                    keyAdapter.checkConstraints(startExpressions, endExpressions, f, null, types);
                }
                S[] startValues = keyAdapter.createSourceArray(startBoundColumns);
                S[] endValues = keyAdapter.createSourceArray(endBoundColumns);
                for (int f = 0; f < startBoundColumns; f++) {
                    startValues[f] = keyAdapter.get(startExpressions, f);
                    endValues[f] = keyAdapter.get(endExpressions, f);
                }
                clearStart();
                clearEnd();
                // Construct bounds of search. For first boundColumns - 1 columns, if start and end are both null,
                // interpret the nulls literally.
                int f = 0;
                while (f < startBoundColumns - 1) {
                    startKey.append(startValues[f], type(f));
                    endKey.append(endValues[f], type(f));
                    f++;
                }
                
                // checking for values which are null, but not literal nulls
                // SELECT * FROM t where x = ? (parameter NULL) should return no rows
                // SELECT * FROM t where x is NULL which should. 
                // if this case is true, there will not be any rows returned, so
                // idle the cursor, and return no rows quickly.
                if (keyAdapter.isNull(startValues[f]) && !start.isLiteralNull(f)) {
                    LOG.debug("Found stop case for non-literal null in start");
                    setIdle();
                    return;
                }
                endValues[f] = keyAdapter.get(endExpressions, f);
                if (keyAdapter.isNull(endValues[f]) && !end.isLiteralNull(f)) {
                    LOG.debug("Found stop case for non-literal null in end");
                    setIdle();
                    return;
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
                    startKey.append(startValues[f], type(f));
                    // End values
                    if (keyAdapter.isNull(endValues[f])) {
                        if (endInclusive) {
                            if (startInclusive && keyAdapter.isNull(startValues[f])) {
                                // Case 10:
                                endKey.append(endValues[f], type(f));
                            } else {
                                // Cases 2, 6, 14:
                                throw new IllegalArgumentException("Bad null ordering in IndexBounds: " + startValues[f] + " to " + endValues[f]);
                            }
                        } else {
                            // Cases 0, 4, 8, 12
                            endKey.append(EdgeValue.AFTER);
                        }
                    } else {
                        // Cases 1, 3, 5, 7, 9, 11, 13, 15
                        endKey.append(endValues[f], type(f));
                    }
                } else {
                    // Same as above, swapping start and end
                    // End values
                    endKey.append(endValues[f], type(f));
                    // Start values
                    if (keyAdapter.isNull(startValues[f])) {
                        if (startInclusive) {
                            if (endInclusive && keyAdapter.isNull(endValues[f])) {
                                // Case 10:
                                startKey.append(startValues[f], type(f));
                            } else {
                                // Cases 2, 6, 14:
                                throw new IllegalArgumentException("Bad null ordering in IndexBounds: " + startValues[f] + " to " + endValues[f]);
                            }
                        } else {
                            // Cases 0, 4, 8, 12
                            startKey.append(EdgeValue.AFTER);
                        }
                    } else {
                        // Cases 1, 3, 5, 7, 9, 11, 13, 15
                        startKey.append(startValues[f], type(f));
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
            ValueRecord startExpressions = start.boundExpressions(context, bindings);
            S[] startValues = keyAdapter.createSourceArray(startBoundColumns);
            for (int f = 0; f < startBoundColumns; f++) {
                startValues[f] = keyAdapter.get(startExpressions, f);
            }
            clearStart();
            
            // Construct bounds of search. For first boundColumns - 1 columns, if start and end are both null,
            // interpret the nulls literally.
            int f = 0;
            while (f < startBoundColumns - 1) {
                startKey.append(startValues[f], type(f));
                f++;
            }
            if (direction == FORWARD) {
                startKey.append(startValues[f], type(f));
            } else {
                if (keyAdapter.isNull(startValues[f])) {
                    if (startInclusive) {
                        // Assume case 10, the only valid choice here. On evaluateBoundaries, cases 2, 6, 14
                        // would have thrown IllegalArgumentException.
                        startKey.append(startValues[f], type(f));
                    } else {
                        // Cases 0, 4, 8, 12
                        startKey.append(EdgeValue.AFTER);
                    }
                } else {
                    // Cases 1, 3, 5, 7, 9, 11, 13, 15
                    startKey.append(startValues[f], type(f));
                }
            }
        }
    }

    // TODO : Once the Persistit storage engine is removed, the FDB storage engine
    // does a correct job of selecting the range, and this method can be removed.
    protected boolean beforeStart(Row row)
    {
        boolean beforeStart = false;
        if (startKey != null && row != null && startBoundColumns != 0) {
            IndexRow current = (IndexRow) row;
            int c = current.compareTo(startKey, startBoundColumns) * direction;
            beforeStart = c < 0 || c == 0 && !startInclusive;
        }
        return beforeStart;
    }

    // TODO : Once the Persistit storage engine is removed, the FDB storage engine
    // does a correct job of selecting the range, and this method can be removed.
    protected boolean pastEnd(Row row)
    {
        boolean pastEnd;
        if (endKey == null || endBoundColumns == 0) {
            pastEnd = false;
        } else {
            IndexRow current = (IndexRow) row;
            int c = current.compareTo(endKey, endBoundColumns) * direction;
            pastEnd = c > 0 || c == 0 && !endInclusive;
        }
        return pastEnd;
    }

    protected void clearStart()
    {
        startKeyKey.clear();
        startKey.resetForWrite(index(), startKeyKey);
    }

    protected void clearEnd()
    {
        endKeyKey.clear();
        endKey.resetForWrite(index(), endKeyKey);
    }

    protected TInstance type(int f)
    {
        return types == null ? null : types[f];
    }

    // For use by this class

    private void initializeCursor()
    {
        this.lo = keyRange.lo();
        this.hi = keyRange.hi();
        if (ordering.allAscending()) {
            this.direction = FORWARD;
            this.start = this.lo;
            this.startInclusive = keyRange.loInclusive();
            this.end = this.hi;
            this.endInclusive = keyRange.hiInclusive();
            this.initialKeyComparison = startInclusive ? Key.GTEQ : Key.GT;
            this.subsequentKeyComparison = Key.GT;
            this.startBoundary = EdgeValue.BEFORE;
        } else if (ordering.allDescending()) {
            this.direction = BACKWARD;
            this.start = this.hi;
            this.startInclusive = keyRange.hiInclusive();
            this.end = this.lo;
            this.endInclusive = keyRange.loInclusive();
            this.initialKeyComparison = startInclusive ? Key.LTEQ : Key.LT;
            this.subsequentKeyComparison = Key.LT;
            this.startBoundary = EdgeValue.AFTER;
        } else {
            assert false : ordering;
        }
        this.startKey = adapter.takeIndexRow(keyRange.indexRowType());
        this.startKeyKey = adapter.getKeyCreator().createKey();
        this.startBoundColumns = keyRange.boundColumns();

        this.endKeyKey = adapter.getKeyCreator().createKey();
        // Set up type info, allowing for spatial indexes
        //this.collators = sortKeyAdapter.createAkCollators(startBoundColumns);
        this.types = sortKeyAdapter.createTInstances(startBoundColumns);
        Index index = keyRange.indexRowType().index();
        int firstSpatialColumn;
        if (index.isSpatial()) {
            firstSpatialColumn = index.firstSpatialArgument();
        } else {
            firstSpatialColumn = Integer.MAX_VALUE;
        }
        List<IndexColumn> indexColumns = index().getAllColumns();
        int logicalColumn = 0;
        int physicalColumn = 0;
        while (logicalColumn < startBoundColumns) {
            if (logicalColumn == firstSpatialColumn) {
                types[physicalColumn] = InternalIndexTypes.LONG.instance(SpatialHelper.isNullable(keyRange));
                logicalColumn += index.spatialColumns();
            } else {
                Column column = indexColumns.get(logicalColumn).getColumn();
                sortKeyAdapter.setColumnMetadata(column, physicalColumn, types);
                logicalColumn++;
            }
            physicalColumn++;
        }
    }

    private void initializeForOpen()
    {
        clear();
        // This can happen if evaluateBoundaries() finds non-literal nulls for the last values
        // of the scan range. If this is true, skip this process.
        if (isIdle()) {
            return; 
        }
        if (startKey != null) {
            // boundColumns > 0 means that startKey has some values other than BEFORE or AFTER. start == null
            // could happen in a lexicographic scan, and indicates no lower bound (so we're starting at BEFORE or AFTER).
            if ((startBoundColumns > 0 && start != null) &&
                (direction == FORWARD && !startInclusive || direction == BACKWARD && startInclusive)) {
                // - direction == FORWARD && !startInclusive: If the search key is (10, 5) and there are
                //   rows (10, 5, ...) then we do not want them if !startInclusive. Making the search key
                //   (10, 5, AFTER) will cause these records to be skipped.
                // - direction == BACKWARD && startInclusive: Similarly, going in the other direction, we do the
                //   (10, 5, ...) records if startInclusive. But an LTEQ traversal would miss it unless we search
                //   for (10, 5, AFTER).
                startKey.append(EdgeValue.AFTER);
            }
            // Copy just the persistit key part of startKey to the exchange's key. startKey may be overspecified.
            // E.g., if we have a PK index for a non-root table, the index row is [childPK, parentPK], and an index
            // scan may specify a value for both. But the persistit search can only deal with the [childPK] part of
            // the traversal.
            startKey.copyPersistitKeyTo(key());
            //Copy endKey into key->FDBIterationHelper ->storeData.endKey 
            // which sets an upper bound on the scan range
            if (endKey != null)
                endKey.copyPersistitKeyTo(endKey());
            pastStart = false;
        }
        keyComparison = initialKeyComparison;
        iterationHelper.preload(keyComparison);
    }

    private Index index()
    {
        return keyRange.indexRowType().index();
    }

    private IndexCursorUnidirectional(QueryContext context,
                                      IterationHelper iterationHelper,
                                      API.Ordering ordering,
                                      SortKeyAdapter<S, ?> sortKeyAdapter)
    {
        super(context, iterationHelper);
        this.initialKeyRange = null;
        this.ordering = ordering;
        if (ordering.allAscending()) {
            this.startBoundary = EdgeValue.BEFORE;
            this.initialKeyComparison = Key.GT;
            this.subsequentKeyComparison = Key.GT;
        } else if (ordering.allDescending()) {
            this.startBoundary = EdgeValue.AFTER;
            this.initialKeyComparison = Key.LT;
            this.subsequentKeyComparison = Key.LT;
        } else {
            assert false : ordering;
        }
        this.startKey = null;
        this.endKey = null;
        this.startBoundColumns = 0;
        this.endBoundColumns = 0;
        this.sortKeyAdapter = sortKeyAdapter;
    }

    // Class state

    private static final int FORWARD = 1;
    private static final int BACKWARD = -1;

    // Object state

    private final IndexKeyRange initialKeyRange;
    private final API.Ordering ordering;
    private IndexKeyRange keyRange;
    protected int direction; // +1 = ascending, -1 = descending
    protected Key.Direction keyComparison;
    protected Key.Direction initialKeyComparison;
    protected Key.Direction subsequentKeyComparison;
    protected EdgeValue startBoundary; // Start of a scan that is unbounded at the start
    // start/endBoundColumns is the number of index fields with restrictions. They start out having the same value.
    // But jump(Row) resets state pertaining to the start of a scan, including startBoundColumns.
    protected int startBoundColumns;
    protected int endBoundColumns;
    protected TInstance[] types;
    //protected AkCollator[] collators;
    protected IndexBound lo;
    protected IndexBound hi;
    protected IndexBound start;
    protected IndexBound end;
    protected boolean startInclusive;
    protected boolean endInclusive;
    protected IndexRow startKey;
    protected IndexRow endKey;
    private Key startKeyKey;
    private Key endKeyKey;
    private boolean pastStart;
    private SortKeyAdapter<S, ?> sortKeyAdapter;
    private static final Logger LOG = LoggerFactory.getLogger(IndexCursorUnidirectional.class);
}
