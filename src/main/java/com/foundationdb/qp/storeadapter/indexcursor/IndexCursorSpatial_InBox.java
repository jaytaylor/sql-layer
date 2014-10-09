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
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.InternalIndexTypes;
import com.foundationdb.qp.util.MultiCursor;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.server.api.dml.IndexRowPrefixSelector;
import com.foundationdb.server.spatial.BoxLatLon;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.common.types.TBigDecimal;
import com.foundationdb.server.types.texpressions.TPreparedField;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueRecord;
import com.foundationdb.server.types.value.ValueSource;
import com.geophile.z.Space;
import com.geophile.z.SpatialObject;

import java.util.ArrayList;
import java.util.List;

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
        return  new IndexCursorSpatial_InBox(context, iterationHelper, keyRange, openAll);
    }

    // For use by this class

    private IndexCursorSpatial_InBox(QueryContext context, IterationHelper iterationHelper, IndexKeyRange keyRange, boolean openAll)
    {
        super(context, iterationHelper);
        assert keyRange.spatial();
        this.multiCursor = new MultiCursor(openAll);
        this.iterationHelper = iterationHelper;
        Index spatialIndex = keyRange.indexRowType().index();
        assert spatialIndex.isSpatial() : spatialIndex;
        this.space = spatialIndex.space();
        this.latColumn = spatialIndex.firstSpatialArgument();
        this.lonColumn = latColumn + 1;
        API.Ordering zOrdering = new API.Ordering();
        IndexRowType rowType = keyRange.indexRowType().physicalRowType();
        for (int f = 0; f < rowType.nFields(); f++) {
            zOrdering.append(new TPreparedField(rowType.typeAt(f), f), true);
        }
        // The index column selector needs to select all the columns before the z column, and the z column itself.
        this.indexColumnSelector = new IndexRowPrefixSelector(this.latColumn + 1);
        for (IndexKeyRange zKeyRange : zKeyRanges(context, keyRange)) {
            IterationHelper rowState = adapter.createIterationHelper(keyRange.indexRowType());

            IndexCursorUnidirectional<ValueSource> zIntervalCursor =
                new IndexCursorUnidirectional<>(context,
                                                            rowState,
                                                            zKeyRange,
                                                            zOrdering,
                                                            ValueSortKeyAdapter.INSTANCE);
            multiCursor.addCursor(zIntervalCursor);
        }
    }

    private List<IndexKeyRange> zKeyRanges(QueryContext context, IndexKeyRange keyRange)
    {
        List<IndexKeyRange> zKeyRanges = new ArrayList<>();
        Index index = keyRange.indexRowType().index();
        IndexBound loBound = keyRange.lo();
        IndexBound hiBound = keyRange.hi();
        ValueRecord loExpressions = loBound.boundExpressions(context, bindings);
        ValueRecord hiExpressions = hiBound.boundExpressions(context, bindings);
        // Only 2d, lat/lon supported for now
        double xLo, xHi, yLo, yHi;
        TInstance xinst = index.getAllColumns().get(latColumn).getColumn().getType();
        TInstance yinst = index.getAllColumns().get(lonColumn).getColumn().getType();
        xLo = TBigDecimal.getWrapper(loExpressions.value(latColumn), xinst).asBigDecimal().doubleValue();
        xHi = TBigDecimal.getWrapper(hiExpressions.value(latColumn), xinst).asBigDecimal().doubleValue();
        yLo = TBigDecimal.getWrapper(loExpressions.value(lonColumn), yinst).asBigDecimal().doubleValue();
        yHi = TBigDecimal.getWrapper(hiExpressions.value(lonColumn), yinst).asBigDecimal().doubleValue();
        SpatialObject box = BoxLatLon.newBox(xLo, xHi, yLo, yHi);
        long[] zValues = new long[box.maxZ()];
        space.decompose(box, zValues);
        for (int i = 0; i < zValues.length; i++) {
            long z = zValues[i];
            if (z != -1L) {
                IndexRowType physicalRowType = keyRange.indexRowType().physicalRowType();
                int indexRowFields = physicalRowType.nFields();
                SpatialIndexValueRecord zLoRow = new SpatialIndexValueRecord(indexRowFields);
                SpatialIndexValueRecord zHiRow = new SpatialIndexValueRecord(indexRowFields);
                IndexBound zLo = new IndexBound(zLoRow, indexColumnSelector);
                IndexBound zHi = new IndexBound(zHiRow, indexColumnSelector);
                // Take care of any equality restrictions before the spatial fields
                for (int f = 0; f < latColumn; f++) {
                    ValueSource eqValueSource = loExpressions.value(f);
                    zLoRow.value(f, eqValueSource);
                    zHiRow.value(f, eqValueSource);
                }
                // lo and hi bounds
                Value loValue = new Value(InternalIndexTypes.LONG.instance(false));
                Value hiValue = new Value(InternalIndexTypes.LONG.instance(false));
                loValue.putInt64(Space.zLo(z));
                hiValue.putInt64(Space.zHi(z));
                zLoRow.value(latColumn, loValue);
                zHiRow.value(latColumn, hiValue);
                IndexKeyRange zKeyRange = IndexKeyRange.bounded(physicalRowType, zLo, true, zHi, true);
                zKeyRanges.add(zKeyRange);
            }
        }
        return zKeyRanges;
    }

    // Class state

    // Object state

    private final Space space;
    private final ColumnSelector indexColumnSelector;
    private final int latColumn;
    private final int lonColumn;
    private final MultiCursor multiCursor;
    private final IterationHelper iterationHelper;
}
