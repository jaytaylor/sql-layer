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

package com.akiban.qp.persistitadapter;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.IndexColumn;
import com.akiban.qp.expression.BoundExpressions;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.CursorLifecycle;
import com.akiban.qp.operator.IndexScanSelector;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.persistitadapter.indexcursor.PValueSortKeyAdapter;
import com.akiban.qp.persistitadapter.indexcursor.SortKeyAdapter;
import com.akiban.qp.persistitadapter.indexrow.PersistitIndexRow;
import com.akiban.qp.persistitadapter.indexrow.PersistitTableIndexRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.collation.AkCollator;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.FDBStore;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.texpressions.TPreparedExpression;
import com.foundationdb.KeyValue;
import com.foundationdb.tuple.Tuple;
import com.persistit.Key;
import com.persistit.Persistit;
import com.persistit.Value;

import java.util.Iterator;
import java.util.List;

class FDBIndexCursor implements Cursor
{
    private final FDBAdapter adapter;
    private final QueryContext context;
    private final IndexRowType indexRowType;
    private final IndexKeyRange keyRange;
    private final API.Ordering ordering;
    private final IndexScanSelector selector;
    private boolean idle;
    private boolean destroyed;
    private Iterator<KeyValue> indexIt = null;


    FDBIndexCursor(FDBAdapter adapter,
                   QueryContext context,
                   IndexRowType indexRowType,
                   IndexKeyRange keyRange,
                   API.Ordering ordering,
                   IndexScanSelector selector) {
        this.adapter = adapter;
        this.keyRange = keyRange;
        this.ordering = ordering;
        this.context = context;
        this.indexRowType = indexRowType;
        this.selector = selector;
        this.idle = true;
        this.destroyed = false;
        if(!(ordering.allAscending() || ordering.allDescending())) {
            throw new UnsupportedOperationException("Only ALL asc or ALL desc index scan supported");
        }
        if(!indexRowType.index().isTableIndex()) {
            throw new UnsupportedOperationException("Only TableIndex index scan supported");
        }
        if(!selector.matchesAll()) {
            throw new UnsupportedOperationException("Only matchesAll selector index scan supported");
        }
    }

    // TODO: Hacked from IndeCursorUnidirectional
    private void simpleEvaluateBoundaries(Key startKeyKey, Key endKeyKey) {
        SortKeyAdapter<PValueSource,TPreparedExpression> keyAdapter = PValueSortKeyAdapter.INSTANCE;
        IndexBound lo = keyRange.lo();
        IndexBound hi = keyRange.hi();
        int startBoundColumns = keyRange.boundColumns();
        int endBoundColumns = startBoundColumns;
        TInstance[] tInstances = keyAdapter.createTInstances(startBoundColumns);
        AkCollator[] collators = keyAdapter.createAkCollators(startBoundColumns);
        PersistitIndexRow startKey = new PersistitTableIndexRow(adapter, indexRowType);
        PersistitIndexRow endKey = new PersistitTableIndexRow(adapter, indexRowType);
        boolean startInclusive = keyRange.loInclusive();
        boolean endInclusive = keyRange.hiInclusive();

        List<IndexColumn> indexColumns = indexRowType.index().getAllColumns();
        int logicalColumn = 0;
        while (logicalColumn < startBoundColumns) {
            Column column = indexColumns.get(logicalColumn).getColumn();
            keyAdapter.setColumnMetadata(column, logicalColumn, null, collators, tInstances);
            logicalColumn++;
        }

        if(collators == null) {
            collators = new AkCollator[startBoundColumns];
        }

        // Check constraints on start and end
        BoundExpressions loExpressions = lo.boundExpressions(context);
        BoundExpressions hiExpressions = hi.boundExpressions(context);
        for (int f = 0; f < endBoundColumns - 1; f++) {
            keyAdapter.checkConstraints(loExpressions, hiExpressions, f, collators, tInstances);
        }
        // Construct start and end keys
        BoundExpressions startExpressions = lo.boundExpressions(context);
        BoundExpressions endExpressions = hi.boundExpressions(context);
        // startBoundColumns == endBoundColumns because jump() hasn't been called.
        // If it had we'd be in reevaluateBoundaries, not here.
        PValueSource[] startValues = keyAdapter.createSourceArray(startBoundColumns);
        PValueSource[] endValues = keyAdapter.createSourceArray(endBoundColumns);
        for (int f = 0; f < startBoundColumns; f++) {
            startValues[f] = keyAdapter.get(startExpressions, f);
            endValues[f] = keyAdapter.get(endExpressions, f);
        }
        startKeyKey.clear();
        startKey.resetForWrite(indexRowType.index(), startKeyKey, null);
        endKeyKey.clear();
        endKey.resetForWrite(indexRowType.index(), endKeyKey, null);

        // Construct bounds of search. For first boundColumns - 1 columns, if start and end are both null,
        // interpret the nulls literally.
        int f = 0;
        while (f < startBoundColumns - 1) {
            startKey.append(startValues[f], null, tInstances[f], collators[f]);
            endKey.append(startValues[f], null, tInstances[f], collators[f]);
            f++;
        }

        // FORWARD only, FDB request gets reverse flag if needed

        // Start values
        startKey.append(startValues[f], null, tInstances[f], collators[f]);

        // End values
        if (keyAdapter.isNull(endValues[f])) {
            if (endInclusive) {
                if (startInclusive && keyAdapter.isNull(startValues[f])) {
                    // Case 10:
                    endKey.append(endValues[f], null, tInstances[f], collators[f]);
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
            endKey.append(endValues[f], null, tInstances[f], collators[f]);
        }

        if(!startInclusive) {
            startKey.append(Key.AFTER);
        }
        if(endInclusive) {
            endKey.append(Key.AFTER);
        }
    }



    @Override
    public void open() {
        CursorLifecycle.checkIdle(this);
        idle = false;

        boolean reverse = ordering.allDescending();
        FDBStore store = adapter.getUnderlyingStore();
        if(keyRange.unbounded()) {
            indexIt = store.indexIterator(adapter.getSession(), indexRowType.index(), reverse);
        } else {
            Key startKey = adapter.getUnderlyingStore().createKey();
            Key endKey = adapter.getUnderlyingStore().createKey();
            simpleEvaluateBoundaries(startKey, endKey);
            indexIt = store.indexIterator(adapter.getSession(), indexRowType.index(), startKey, endKey, reverse);
        }
    }

    @Override
    public Row next() {
        CursorLifecycle.checkIdleOrActive(this);
        PersistitTableIndexRow row = null;
        if(indexIt.hasNext()) {
            KeyValue kv = indexIt.next();

            byte[] keyBytes = Tuple.fromBytes(kv.getKey()).getBytes(2);
            Key key = adapter.getUnderlyingStore().createKey();
            System.arraycopy(keyBytes, 0, key.getEncodedBytes(), 0, keyBytes.length);
            key.setEncodedSize(keyBytes.length);

            byte[] valueBytes = kv.getValue();
            Value value = new Value((Persistit)null);
            value.putByteArray(valueBytes);

            row = new PersistitTableIndexRow(adapter, indexRowType);
            row.copyFromKeyValue(key, value);
        } else {
            close();
            idle = true;
        }
        return row;
    }

    @Override
    public void jump(Row row, ColumnSelector columnSelector) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        CursorLifecycle.checkIdleOrActive(this);
        idle = true;
    }

    @Override
    public void destroy() {
        destroyed = true;
    }

    @Override
    public boolean isIdle() {
        return !destroyed && idle;
    }

    @Override
    public boolean isActive() {
        return !destroyed && !idle;
    }

    @Override
    public boolean isDestroyed() {
        return destroyed;
    }
}
