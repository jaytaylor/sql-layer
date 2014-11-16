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

import com.foundationdb.ais.model.Index;
import com.foundationdb.qp.expression.IndexBound;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.RowCursorImpl;
import com.foundationdb.qp.row.IndexRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.InternalIndexTypes;
import com.foundationdb.qp.util.MultiCursor;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.server.api.dml.IndexRowPrefixSelector;
import com.foundationdb.server.spatial.BoxLatLon;
import com.foundationdb.server.spatial.GeophileCursor;
import com.foundationdb.server.spatial.GeophileIndex;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.common.types.TBigDecimal;
import com.foundationdb.server.types.texpressions.TPreparedField;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueRecord;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.util.IteratorToCursorAdapter;
import com.geophile.z.Space;
import com.geophile.z.SpatialIndex;
import com.geophile.z.SpatialJoin;
import com.geophile.z.SpatialObject;
import com.geophile.z.space.SpaceImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

// A scan of an IndexCursorSpatial_InBox will be implemented as one or more IndexCursorUnidirectional scans.

class IndexCursorSpatial_InBox extends IndexCursor
{
    @Override
    public void open()
    {
        super.open();
        // iterationHelper.closeIteration() closes the PersistitIndexCursor, releasing its Exchange.
        // This iteration uses the Exchanges in the IndexScanRowStates owned by each cursor of the MultiCursor.
        iterationHelper.closeIteration();
        multiCursor.open();
    }

    @Override
    public Row next()
    {
        super.next();
        return multiCursor.next();
    }

    @Override
    public void close()
    {
        super.close();
        multiCursor.close();
    }

    @Override
    public void rebind(QueryBindings bindings)
    {
        super.rebind(bindings);
        multiCursor.rebind(bindings);
    }

    // IndexCursorSpatial_InBox interface

    public static IndexCursorSpatial_InBox create(QueryContext context,
                                                  IterationHelper iterationHelper,
                                                  IndexKeyRange keyRange,
                                                  boolean openAll)
    {
        return new IndexCursorSpatial_InBox(context, iterationHelper, keyRange, openAll);
    }

    // For use by this class

    private IndexCursorSpatial_InBox(QueryContext context,
                                     IterationHelper iterationHelper,
                                     IndexKeyRange keyRange,
                                     boolean openEarly)
    {
        super(context, iterationHelper);
        this.keyRange = keyRange;
        this.index = keyRange.indexRowType().index();
        assert keyRange.spatial();
        assert index.isSpatial() : index;
        this.space = spatialIndex.space();
        this.loExpressions = keyRange.lo().boundExpressions(context, bindings);
        this.hiExpressions = keyRange.hi().boundExpressions(context, bindings);
        this.iterationHelper = iterationHelper;
        API.Ordering zOrdering = new API.Ordering();
        IndexRowType rowType = keyRange.indexRowType().physicalRowType();
        for (int f = 0; f < rowType.nFields(); f++) {
            zOrdering.append(new TPreparedField(rowType.typeAt(f), f), true);
        }
        // The index column selector needs to select all the columns before the z column, and the z column itself.
        this.indexColumnSelector = new IndexRowPrefixSelector(this.index.firstSpatialArgument() + 1);
        GeophileIndex<IndexRow> geophileIndex = new GeophileIndex<>(adapter, keyRange.indexRowType(), openEarly);
        GeophileCursor<IndexRow> geophileCursor = new GeophileCursor<>(geophileIndex, openEarly);
        for (Map.Entry<Long, IndexKeyRange> entry : zKeyRanges(keyRange).entrySet()) {
            long z = entry.getKey();
            IndexKeyRange zKeyRange = entry.getValue();
            IterationHelper rowState = adapter.createIterationHelper(keyRange.indexRowType());
            IndexCursorUnidirectional<ValueSource> zIntervalCursor =
                new IndexCursorUnidirectional<>(context,
                                                rowState,
                                                zKeyRange,
                                                zOrdering,
                                                ValueSortKeyAdapter.INSTANCE);
            geophileCursor.addCursor(z, zIntervalCursor);
        }
    }

    private Map<Long, IndexKeyRange> zKeyRanges(IndexKeyRange keyRange)
    {
        Map<Long, IndexKeyRange> zKeyRanges = new HashMap<>();
        SpatialObject spatialObject = spatialObject();
        long[] zValues = new long[MAX_Z];
        space.decompose(spatialObject, zValues);
        int zColumn = index.firstSpatialArgument();
        Value hiValue = new Value(InternalIndexTypes.LONG.instance(false));
        hiValue.putInt64(Long.MAX_VALUE);
        for (int i = 0; i < zValues.length && zValues[i] != SpaceImpl.Z_NULL; i++) {
            IndexRowType physicalRowType = keyRange.indexRowType().physicalRowType();
            int indexRowFields = physicalRowType.nFields();
            SpatialIndexValueRecord zLoRow = new SpatialIndexValueRecord(indexRowFields);
            SpatialIndexValueRecord zHiRow = new SpatialIndexValueRecord(indexRowFields);
            IndexBound zLo = new IndexBound(zLoRow, indexColumnSelector);
            IndexBound zHi = new IndexBound(zHiRow, indexColumnSelector);
            // Take care of any equality restrictions before the spatial fields
            for (int f = 0; f < zColumn; f++) {
                ValueSource eqValueSource = loExpressions.value(f);
                zLoRow.value(f, eqValueSource);
                zHiRow.value(f, eqValueSource);
            }
            // lo and hi bounds
            Value loValue = new Value(InternalIndexTypes.LONG.instance(false));
            loValue.putInt64(Space.zLo(zValues[i]));
            zLoRow.value(zColumn, loValue);
            zHiRow.value(zColumn, hiValue);
            IndexKeyRange zKeyRange = IndexKeyRange.bounded(physicalRowType, zLo, true, zHi, true);
            zKeyRanges.put(zValues[i], zKeyRange);
        }
        return zKeyRanges;
    }

    private SpatialObject spatialObject()
    {
        SpatialObject spatialObject;
        if (index.spatialColumns() == 1) {
            // Spatial object
            ValueRecord expressions = keyRange.lo().boundExpressions(context, bindings);
            spatialObject = (SpatialObject) expressions.value(index.firstSpatialArgument()).getObject();
        } else {
            // lat/lon columns
            int latColumn = index.firstSpatialArgument();
            int lonColumn = latColumn + 1;
            TInstance xinst = index.getAllColumns().get(latColumn).getColumn().getType();
            double xLo = TBigDecimal.getWrapper(loExpressions.value(latColumn), xinst).asBigDecimal().doubleValue();
            double xHi = TBigDecimal.getWrapper(hiExpressions.value(latColumn), xinst).asBigDecimal().doubleValue();
            TInstance yinst = index.getAllColumns().get(lonColumn).getColumn().getType();
            double yLo = TBigDecimal.getWrapper(loExpressions.value(lonColumn), yinst).asBigDecimal().doubleValue();
            double yHi = TBigDecimal.getWrapper(hiExpressions.value(lonColumn), yinst).asBigDecimal().doubleValue();
            spatialObject = BoxLatLon.newBox(xLo, xHi, yLo, yHi);
        }
        return spatialObject;
    }

    private SpatialIndex<IndexRow> spatialIndex(boolean openEarly) throws IOException, InterruptedException
    {
        GeophileIndex<IndexRow> geophileIndex = new GeophileIndex<>(adapter, keyRange.indexRowType(), openEarly);
        return SpatialIndex.newSpatialIndex(space, geophileIndex);
    }

    private RowCursorImpl spatialJoinIterator(SpatialIndex<IndexRow> spatialIndex, SpatialObject queryObject)
        throws IOException, InterruptedException
    {
        SpatialJoin spatialJoin = SpatialJoin.newSpatialJoin(SPATIAL_JOIN_DUPLICATION);
        Iterator<IndexRow> spatialJoinIterator = spatialJoin.iterator(queryObject, spatialIndex);
        return new IteratorToCursorAdapter(spatialJoinIterator);
    }

    // Class state

    private static final SpatialJoin.Duplicates SPATIAL_JOIN_DUPLICATION = SpatialJoin.Duplicates.INCLUDE;
    private static final int MAX_Z = 4;

    // Object state

    private final Space space;
    private final Index index;
    private final IndexKeyRange keyRange;
    private final ValueRecord loExpressions;
    private final ValueRecord hiExpressions;
    private final ColumnSelector indexColumnSelector;
    private final MultiCursor multiCursor;
    private final IterationHelper iterationHelper;
}
