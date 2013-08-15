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

package com.foundationdb.qp.persistitadapter.indexcursor;

import com.foundationdb.ais.model.Index;
import com.foundationdb.qp.expression.BoundExpressions;
import com.foundationdb.qp.expression.IndexBound;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.util.MultiCursor;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.server.api.dml.IndexRowPrefixSelector;
import com.foundationdb.server.geophile.BoxLatLon;
import com.foundationdb.server.geophile.Space;
import com.foundationdb.server.geophile.SpaceLatLon;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.types.util.ValueHolder;
import com.foundationdb.server.types3.TInstance;
import com.foundationdb.server.types3.Types3Switch;
import com.foundationdb.server.types3.mcompat.mtypes.MBigDecimal;
import com.foundationdb.server.types3.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types3.pvalue.PValue;
import com.foundationdb.server.types3.pvalue.PValueSource;
import com.foundationdb.server.types3.texpressions.TPreparedField;

import java.math.BigDecimal;
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
    public void destroy()
    {
        super.destroy();
        multiCursor.destroy();
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
            zOrdering.append(new TPreparedField(rowType.typeInstanceAt(f), f), true);
        }
        // The index column selector needs to select all the columns before the z column, and the z column itself.
        this.indexColumnSelector = new IndexRowPrefixSelector(this.latColumn + 1);
        for (IndexKeyRange zKeyRange : zKeyRanges(context, keyRange)) {
            IterationHelper rowState = adapter.createIterationHelper(keyRange.indexRowType());
            if (Types3Switch.ON) {
                IndexCursorUnidirectional<PValueSource> zIntervalCursor =
                    new IndexCursorUnidirectional<>(context,
                                                                rowState,
                                                                zKeyRange,
                                                                zOrdering,
                                                                PValueSortKeyAdapter.INSTANCE);
                multiCursor.addCursor(zIntervalCursor);
            }
            else {
                IndexCursorUnidirectional<ValueSource> zIntervalCursor =
                    new IndexCursorUnidirectional<>(context,
                                                               rowState,
                                                               zKeyRange,
                                                               zOrdering,
                                                               OldExpressionsSortKeyAdapter.INSTANCE);
                multiCursor.addCursor(zIntervalCursor);
            }
        }
    }

    private List<IndexKeyRange> zKeyRanges(QueryContext context, IndexKeyRange keyRange)
    {
        List<IndexKeyRange> zKeyRanges = new ArrayList<>();
        Index index = keyRange.indexRowType().index();
        IndexBound loBound = keyRange.lo();
        IndexBound hiBound = keyRange.hi();
        BoundExpressions loExpressions = loBound.boundExpressions(context, bindings);
        BoundExpressions hiExpressions = hiBound.boundExpressions(context, bindings);
        // Only 2d, lat/lon supported for now
        BigDecimal xLo, xHi, yLo, yHi;
        if (Types3Switch.ON) {
            TInstance xinst = index.getAllColumns().get(latColumn).getColumn().tInstance();
            TInstance yinst = index.getAllColumns().get(lonColumn).getColumn().tInstance();
            xLo = MBigDecimal.getWrapper(loExpressions.pvalue(latColumn), xinst).asBigDecimal();
            xHi = MBigDecimal.getWrapper(hiExpressions.pvalue(latColumn), xinst).asBigDecimal();
            yLo = MBigDecimal.getWrapper(loExpressions.pvalue(lonColumn), yinst).asBigDecimal();
            yHi = MBigDecimal.getWrapper(hiExpressions.pvalue(lonColumn), yinst).asBigDecimal();
        } else {
            xLo = loExpressions.eval(latColumn).getDecimal();
            xHi = hiExpressions.eval(latColumn).getDecimal();
            yLo = loExpressions.eval(lonColumn).getDecimal();
            yHi = hiExpressions.eval(lonColumn).getDecimal();
        }
        BoxLatLon box = BoxLatLon.newBox(xLo, xHi, yLo, yHi);
        long[] zValues = new long[SpaceLatLon.MAX_DECOMPOSITION_Z_VALUES];
        space.decompose(box, zValues);
        for (int i = 0; i < zValues.length; i++) {
            long z = zValues[i];
            if (z != -1L) {
                IndexRowType physicalRowType = keyRange.indexRowType().physicalRowType();
                int indexRowFields = physicalRowType.nFields();
                SpatialIndexBoundExpressions zLoRow = new SpatialIndexBoundExpressions(indexRowFields);
                SpatialIndexBoundExpressions zHiRow = new SpatialIndexBoundExpressions(indexRowFields);
                IndexBound zLo = new IndexBound(zLoRow, indexColumnSelector);
                IndexBound zHi = new IndexBound(zHiRow, indexColumnSelector);
                // Take care of any equality restrictions before the spatial fields
                for (int f = 0; f < latColumn; f++) {
                    if (Types3Switch.ON) {
                        PValueSource eqValueSource = loExpressions.pvalue(f);
                        zLoRow.value(f, eqValueSource);
                        zHiRow.value(f, eqValueSource);
                    } else {
                        ValueSource eqValue = loExpressions.eval(f);
                        zLoRow.value(f, eqValue);
                        zHiRow.value(f, eqValue);
                    }
                }
                // lo and hi bounds
                if (Types3Switch.ON) {
                    PValue loPValue = new PValue(MNumeric.BIGINT.instance(false));
                    PValue hiPValue = new PValue(MNumeric.BIGINT.instance(false));
                    loPValue.putInt64(space.zLo(z));
                    hiPValue.putInt64(space.zHi(z));
                    zLoRow.value(latColumn, loPValue);
                    zHiRow.value(latColumn, hiPValue);
                }
                else {
                    ValueSource loValue = new ValueHolder(AkType.LONG, space.zLo(z));
                    ValueSource hiValue = new ValueHolder(AkType.LONG, space.zHi(z));
                    zLoRow.value(latColumn, loValue);
                    zHiRow.value(latColumn, hiValue);
                }
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
