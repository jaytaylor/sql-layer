/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.qp.persistitadapter.sort;

import com.akiban.ais.model.IndexColumn;
import com.akiban.qp.expression.BoundExpressions;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Bindings;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.persistitadapter.PersistitAdapterException;
import com.akiban.qp.row.Row;
import com.akiban.server.PersistitKeyValueTarget;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.std.Comparison;
import com.akiban.server.expression.std.Expressions;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.conversion.Converters;
import com.persistit.Key;
import com.persistit.exception.PersistitException;

import java.util.List;

public class SortCursorUnidirectional extends SortCursor
{
    // Cursor interface

    @Override
    public void open(Bindings bindings)
    {
        exchange.clear();
        if (bounded) {
            evaluateBoundaries(bindings);
            if (startKey == null) {
                exchange.append(startBoundary);
            } else {
                if (direction == FORWARD && !startInclusive || direction == BACKWARD && startInclusive) {
                    // direction == 1 && !startInclusive: If the search key is (10, 5) and there are rows (10, 5, ...)
                    // then we do not want them if !startInclusive. Making the search key (10, 5, AFTER) will cause
                    // these records to be skipped.
                    // direction == -1 && startInclusive: Similarly, going in the other direction, we do the
                    // (10, 5, ...) records if startInclusive. But an LTEQ traversal would miss it unless we search
                    // for (10, 5, AFTER).
                    startKey.append(Key.AFTER);
                }
                startKey.copyTo(exchange.getKey());
            }
        } else {
            exchange.append(startBoundary);
        }
    }

    @Override
    public Row next()
    {
        Row next = null;
        if (exchange != null) {
            try {
                if (exchange.traverse(keyComparison, true)) {
                    next = row();
                    if (bounded) {
                        if (pastEnd()) {
                            next = null;
                            close();
                        } else {
                            keyComparison = subsequentKeyComparison;
                        }
                    }
                } else {
                    close();
                }
            } catch (PersistitException e) {
                close();
                throw new PersistitAdapterException(e);
            }
        }
        return next;
    }

    // SortCursorUnidirectional interface

    public static SortCursorUnidirectional create(PersistitAdapter adapter,
                                                  RowGenerator rowGenerator,
                                                  IndexKeyRange keyRange,
                                                  API.Ordering ordering)
    {
        return
            keyRange == null || keyRange.unbounded()
            ? new SortCursorUnidirectional(adapter, rowGenerator, ordering)
            : new SortCursorUnidirectional(adapter, rowGenerator, keyRange, ordering);
    }

    // For use by this class

    private SortCursorUnidirectional(PersistitAdapter adapter,
                                     RowGenerator rowGenerator,
                                     API.Ordering ordering)
    {
        super(adapter, rowGenerator);
        this.bounded = false;
        this.adapter = adapter;
        if (ordering.allAscending()) {
            this.startBoundary = Key.BEFORE;
            this.keyComparison = Key.GT;
            this.subsequentKeyComparison = Key.GT;
        } else if (ordering.allDescending()) {
            this.startBoundary = Key.AFTER;
            this.keyComparison = Key.LT;
            this.subsequentKeyComparison = Key.LT;
        } else {
            assert false : ordering;
        }
    }

    private SortCursorUnidirectional(PersistitAdapter adapter,
                                     RowGenerator rowGenerator,
                                     IndexKeyRange keyRange,
                                     API.Ordering ordering)
    {
        super(adapter, rowGenerator);
        this.bounded = true;
        this.adapter = adapter;
        this.lo = keyRange.lo();
        this.hi = keyRange.hi();
        if (ordering.allAscending()) {
            this.direction = FORWARD;
            this.start = this.lo;
            this.startInclusive = keyRange.loInclusive();
            this.end = this.hi;
            this.endInclusive = keyRange.hiInclusive();
            this.keyComparison = startInclusive ? Key.GTEQ : Key.GT;
            this.subsequentKeyComparison = Key.GT;
            this.startBoundary = Key.BEFORE;
        } else if (ordering.allDescending()) {
            this.direction = BACKWARD;
            this.start = this.hi;
            this.startInclusive = keyRange.hiInclusive();
            this.end = this.lo;
            this.endInclusive = keyRange.loInclusive();
            this.keyComparison = startInclusive ? Key.LTEQ : Key.LT;
            this.subsequentKeyComparison = Key.LT;
            this.startBoundary = Key.AFTER;
        } else {
            assert false : ordering;
        }
        this.startKey = adapter.newKey();
        this.endKey = adapter.newKey();
        this.boundColumns = keyRange.boundColumns();
        this.types = new AkType[boundColumns];
        List<IndexColumn> indexColumns = keyRange.indexRowType().index().getColumns();
        for (int f = 0; f < boundColumns; f++) {
            this.types[f] = indexColumns.get(f).getColumn().getType().akType();
        }
    }

    private void evaluateBoundaries(Bindings bindings)
    {
        /*
            Null bounds are slightly tricky. An index restriction is described by an IndexKeyRange which contains
            two IndexBounds. The IndexBound wraps an index row. The fields of the row that are being restricted are
            described by the IndexBound's ColumnSelector. The only index restrictions supported specify:
            a) equality for zero or more fields of the index,
            b) 0-1 inequality, and
            c) any remaining columns unbounded.

            By the time we get here, we've stopped paying attention to part c. Parts a and b occupy the first
            sortColumns columns of the index. Now about the nulls: For each field of parts a and b, we have a
            lo value and a hi value. There are four cases:

            - both lo and hi are non-null: Just write the field values into startKey and endKey.

            - lo is null: Write null into the startKey.

            - hi is null, lo is not null: This restriction says that we want everything to the right of
              the lo value. Persistit ranks null lower than anything, so instead of writing null to endKey,
              we write Key.AFTER.

            - lo and hi are both null: This is NOT an unbounded case. This means that we are restricting both
              lo and hi to be null, so write null, not Key.AFTER to endKey.
        */
        boolean[] startNull = new boolean[boundColumns];
        startKey.clear();
        keyTarget.attach(startKey);
        // Check constraints on start and end
        BoundExpressions loExpressions = lo.boundExpressions(bindings, adapter);
        BoundExpressions hiExpressions = hi.boundExpressions(bindings, adapter);
        for (int f = 0; f < boundColumns - 1; f++) {
            Expression loEQHi =
                Expressions.compare(Expressions.valueSource(loExpressions.eval(f)),
                                    Comparison.EQ,
                                    Expressions.valueSource(hiExpressions.eval(f)));
            if (!loEQHi.evaluation().eval().getBool()) {
                throw new IllegalArgumentException();
            }
        }
        Expression loLEHi =
            Expressions.compare(Expressions.valueSource(loExpressions.eval(boundColumns - 1)),
                                Comparison.LE,
                                Expressions.valueSource(hiExpressions.eval(boundColumns - 1)));
        if (!loLEHi.evaluation().eval().getBool()) {
            throw new IllegalArgumentException();
        }
        // Construct start and end keys
        BoundExpressions startExpressions = start.boundExpressions(bindings, adapter);
        for (int f = 0; f < boundColumns; f++) {
            ValueSource keySource = startExpressions.eval(f);
            startNull[f] = keySource.isNull();
            keyTarget.expectingType(types[f]);
            Converters.convert(keySource, keyTarget);
        }
        BoundExpressions endExpressions = end.boundExpressions(bindings, adapter);
        endKey.clear();
        keyTarget.attach(endKey);
        for (int f = 0; f < boundColumns; f++) {
            ValueSource keySource = endExpressions.eval(f);
            if (keySource.isNull() && !startNull[f]) {
                endKey.append(Key.AFTER);
            } else {
                keyTarget.expectingType(types[f]);
                Converters.convert(keySource, keyTarget);
            }
        }
    }

    // TODO: Revisit comparison logic. Checking prefix should work.

    private boolean pastEnd()
    {
        boolean pastEnd;
        if (endKey == null) {
            pastEnd = false;
        } else {
            Key key = exchange.getKey();
            int keyDepth = key.getDepth();
            int keySize = key.getEncodedSize();
            assert key.getDepth() >= endKey.getDepth();
            key.setDepth(endKey.getDepth());
            int c = key.compareTo(endKey) * direction;
            pastEnd = c > 0 || c == 0 && !endInclusive;
            key.setEncodedSize(keySize);
            key.setDepth(keyDepth);
        }
        return pastEnd;
    }

    // Class state

    private static final int FORWARD = 1;
    private static final int BACKWARD = -1;

    // Object state

    private final PersistitAdapter adapter;
    private final boolean bounded; // true for a scan with restrictions, false for a full scan
    private int direction; // +1 = ascending, -1 = descending
    private Key.Direction keyComparison;
    private Key.Direction subsequentKeyComparison;
    private Key.EdgeValue startBoundary; // Start of a scan that is unbounded at the start
    // For bounded scans
    private int boundColumns; // Number of index fields with restrictions
    private AkType[] types;
    private IndexBound lo;
    private IndexBound hi;
    private IndexBound start;
    private IndexBound end;
    private boolean startInclusive;
    private boolean endInclusive;
    private Key startKey;
    private Key endKey;
    private final PersistitKeyValueTarget keyTarget = new PersistitKeyValueTarget();
}
