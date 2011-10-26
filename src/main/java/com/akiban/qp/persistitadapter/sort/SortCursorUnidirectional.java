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
import com.akiban.qp.operator.Bindings;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.persistitadapter.PersistitAdapterException;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.server.PersistitKeyValueTarget;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.conversion.Converters;
import com.persistit.Key;
import com.persistit.exception.PersistitException;

import java.util.List;

public abstract class SortCursorUnidirectional extends SortCursor
{
    // Cursor interface

    @Override
    public void open(Bindings bindings)
    {
        exchange.clear();
        if (bounded) {
            startKey.clear();
            endKey.clear();
            startTarget.attach(startKey);
            endTarget.attach(endKey);
            BoundExpressions startExpressions = start.boundExpressions(bindings, adapter);
            BoundExpressions endExpressions = end.boundExpressions(bindings, adapter);
            for (int f = 0; f < sortFields; f++) {
                ValueSource startSource = startExpressions.eval(f);
                ValueSource endSource = endExpressions.eval(f);
                startTarget.expectingType(types[f]);
                endTarget.expectingType(types[f]);
                Converters.convert(startSource, startTarget);
                Converters.convert(endSource, endTarget);
            }
            if (direction == 1 && !startInclusive || direction == -1 && startInclusive) {
                // direction == 1 && !startInclusive: If the search key is (10, 5) and there is a row (10, 5, ...)
                // then we do not want that row if !startInclusive. Making the search key (10, 5, AFTER) will cause
                // that record to be skipped.
                // direction == -1 && startInclusive: Similarly, going in the other direction, we do want the
                // (10, 5, ...) record if startInclusive. But an LTEQ traversal would miss it unless we search
                // for (10, 5, AFTER).
                startKey.append(Key.AFTER);
            }
            startKey.copyTo(exchange.getKey());
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

    protected SortCursorUnidirectional(PersistitAdapter adapter,
                                       RowGenerator rowGenerator,
                                       Key.EdgeValue startBoundary,
                                       Key.Direction direction)
    {
        super(adapter, rowGenerator);
        this.bounded = false;
        this.adapter = adapter;
        this.startBoundary = startBoundary;
        this.keyComparison = direction;
        this.subsequentKeyComparison = direction;
    }

    protected SortCursorUnidirectional(PersistitAdapter adapter,
                                       RowGenerator rowGenerator,
                                       IndexRowType indexRowType,
                                       int sortFields,
                                       IndexBound start,
                                       boolean startInclusive,
                                       IndexBound end,
                                       boolean endInclusive,
                                       int direction)
    {
        super(adapter, rowGenerator);
        this.bounded = true;
        this.adapter = adapter;
        this.sortFields = sortFields;
        this.start = start;
        this.startInclusive = startInclusive;
        this.end = end;
        this.endInclusive = endInclusive;
        this.startKey = adapter.newKey();
        this.endKey = adapter.newKey();
        this.types = new AkType[sortFields];
        List<IndexColumn> indexColumns = indexRowType.index().getColumns();
        for (int f = 0; f < sortFields; f++) {
            this.types[f] = indexColumns.get(f).getColumn().getType().akType();
        }
        if (direction == 1) {
            this.direction = 1;
            this.keyComparison = startInclusive ? Key.GTEQ : Key.GT;
            this.subsequentKeyComparison = Key.GT;
        } else if (direction == -1) {
            this.direction = -1;
            this.keyComparison = startInclusive ? Key.LTEQ : Key.LT;
            this.subsequentKeyComparison = Key.LT;
        }
    }

    // For use by this class

    // TODO: Revisit comparison logic. Checking prefix should work.

    private boolean pastEnd()
    {
        boolean pastEnd;
        Key key = exchange.getKey();
        int keyDepth = key.getDepth();
        int keySize = key.getEncodedSize();
        assert key.getDepth() >= endKey.getDepth();
        key.setDepth(endKey.getDepth());
        int c = key.compareTo(endKey) * direction;
        pastEnd = c > 0 || c == 0 && !endInclusive;
        key.setEncodedSize(keySize);
        key.setDepth(keyDepth);
        return pastEnd;
    }

    // Object state

    private final PersistitKeyValueTarget startTarget = new PersistitKeyValueTarget();
    private final PersistitKeyValueTarget endTarget = new PersistitKeyValueTarget();
    private final PersistitAdapter adapter;
    private final boolean bounded;
    private int sortFields;
    private int direction; // +1 = ascending, -1 = descending
    private Key.Direction keyComparison;
    // unbounded
    private Key.EdgeValue startBoundary;
    // bounded
    private AkType[] types;
    private Key.Direction subsequentKeyComparison;
    private IndexBound start;
    private IndexBound end;
    private boolean startInclusive;
    private boolean endInclusive;
    private Key startKey;
    private Key endKey;
}
