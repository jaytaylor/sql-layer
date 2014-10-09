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
import com.foundationdb.server.types.value.ValueRecord;
import com.foundationdb.qp.expression.IndexBound;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.BindingsAwareCursor;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.InternalIndexTypes;
import com.foundationdb.server.api.dml.IndexRowPrefixSelector;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.common.types.TBigDecimal;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.server.spatial.Spatial;
import com.geophile.z.Space;

import java.math.BigDecimal;

import static java.lang.Math.abs;

// An IndexCursorSpatial_NearPoint yields points near a given point in z-order. This requires a merge
// of those with a lower z-value and those with a higher z-value.

class IndexCursorSpatial_NearPoint extends IndexCursor
{
    @Override
    public void open()
    {
        super.open();
        // iterationHelper.closeIteration() closes the PersistitIndexCursor, releasing its Exchange.
        iterationHelper.closeIteration();
        geCursor.open();
        geNeedToAdvance = true;
        ltCursor.open();
        ltNeedToAdvance = true;
    }

    @Override
    public Row next()
    {
        Row next;
        super.next();
        if (geNeedToAdvance) {
            geRow = geCursor.next();
            geDistance = distanceFromStart(geRow);
            geNeedToAdvance = false;
        } 
        if (ltNeedToAdvance) {
            ltRow = ltCursor.next();
            ltDistance = distanceFromStart(ltRow);
            ltNeedToAdvance = false;
        }
        if (ltDistance < geDistance) {
            next = ltRow;
            ltNeedToAdvance = true;
        } else {
            next = geRow;
            geNeedToAdvance = true;
        }
        if (next == null) {
            setIdle();
        }
        return next;
    }

    @Override
    public void close()
    {
        if (isActive()) {
            super.close();
            geCursor.close();
            ltCursor.close();
        }
    }

    @Override
    public void rebind(QueryBindings bindings)
    {
        super.rebind(bindings);
        geCursor.rebind(bindings);
        ltCursor.rebind(bindings);
    }

    // IndexCursorSpatial_InBox interface

    public static IndexCursorSpatial_NearPoint create(QueryContext context,
                                                      IterationHelper iterationHelper,
                                                      IndexKeyRange keyRange)
    {
        return new IndexCursorSpatial_NearPoint(context, iterationHelper, keyRange);
    }

    // For use by this class

    private IndexCursorSpatial_NearPoint(QueryContext context, IterationHelper iterationHelper, IndexKeyRange keyRange)
    {
        super(context, iterationHelper);
        assert keyRange.spatial();
        this.iterationHelper = iterationHelper;
        IndexRowType physicalIndexRowType = keyRange.indexRowType().physicalRowType();
        Index index = keyRange.indexRowType().index();
        Space space = index.space();
        int latColumn = index.firstSpatialArgument();
        int lonColumn = latColumn + 1;
        // The index column selector needs to select all the columns before the z column, and the z column itself.
        IndexRowPrefixSelector indexColumnSelector = new IndexRowPrefixSelector(latColumn + 1);
        IndexBound loBound = keyRange.lo();
        ValueRecord loExpressions = loBound.boundExpressions(context, bindings);
        // Compute z-value at beginning of forward and backward scans
        TInstance latInstance = index.getAllColumns().get(latColumn).getColumn().getType();
        TInstance lonInstance = index.getAllColumns().get(lonColumn).getColumn().getType();
        BigDecimal lat = TBigDecimal.getWrapper(loExpressions.value(latColumn), latInstance).asBigDecimal();
        BigDecimal lon = TBigDecimal.getWrapper(loExpressions.value(lonColumn), lonInstance).asBigDecimal();
        zStart = Spatial.shuffle(space, lat.doubleValue(), lon.doubleValue());
        // Cursors going forward from starting z value (inclusive), and backward from the same z value (exclusive)
        int indexRowFields = physicalIndexRowType.nFields();
        SpatialIndexValueRecord zForwardRow = new SpatialIndexValueRecord(indexRowFields);
        SpatialIndexValueRecord zBackwardRow = new SpatialIndexValueRecord(indexRowFields);
        SpatialIndexValueRecord zMaxRow = new SpatialIndexValueRecord(indexRowFields);
        SpatialIndexValueRecord zMinRow = new SpatialIndexValueRecord(indexRowFields);
        IndexBound zForward = new IndexBound(zForwardRow, indexColumnSelector);
        IndexBound zBackward = new IndexBound(zBackwardRow, indexColumnSelector);
        IndexBound zMax = new IndexBound(zMaxRow, indexColumnSelector);
        IndexBound zMin = new IndexBound(zMinRow, indexColumnSelector);
        // Take care of any equality restrictions before the spatial fields
        zPosition = latColumn;
        for (int f = 0; f < zPosition; f++) {
            ValueSource eqValueSource = loExpressions.value(f);
            zForwardRow.value(f, eqValueSource);
            zBackwardRow.value(f, eqValueSource);
            zMaxRow.value(f, eqValueSource);
            zMinRow.value(f, eqValueSource);
        }
        // Z-value part of bounds
        Value startValue = new Value(InternalIndexTypes.LONG.instance(false));
        Value maxValue = new Value(InternalIndexTypes.LONG.instance(false));
        Value minValue = new Value(InternalIndexTypes.LONG.instance(false));
        startValue.putInt64(zStart);
        maxValue.putInt64(Long.MAX_VALUE);
        minValue.putInt64(Long.MIN_VALUE);
        zForwardRow.value(zPosition, startValue);
        zBackwardRow.value(zPosition, startValue);
        zMaxRow.value(zPosition, maxValue);
        zMinRow.value(zPosition, minValue);
        IndexKeyRange geKeyRange = IndexKeyRange.bounded(physicalIndexRowType, zForward, true, zMax, false);
        IndexKeyRange ltKeyRange = IndexKeyRange.bounded(physicalIndexRowType, zMin, false, zBackward, false);
        IterationHelper geRowState = adapter.createIterationHelper(keyRange.indexRowType());
        IterationHelper ltRowState = adapter.createIterationHelper(keyRange.indexRowType());
        API.Ordering upOrdering = new API.Ordering();
        API.Ordering downOrdering = new API.Ordering();
        for (int f = 0; f < physicalIndexRowType.nFields(); f++) {
            // TODO: This seems like an API hack
            upOrdering.append((TPreparedExpression)null, true);
            downOrdering.append((TPreparedExpression)null, false);
        }
        geCursor = new IndexCursorUnidirectional<>(context, 
                                                               geRowState,
                                                               geKeyRange,
                                                               upOrdering,
                                                               ValueSortKeyAdapter.INSTANCE);
        ltCursor = new IndexCursorUnidirectional<>(context, 
                                                               ltRowState,
                                                               ltKeyRange,
                                                               downOrdering,
                                                               ValueSortKeyAdapter.INSTANCE);
    }

    // For use by this class

    private long distanceFromStart(Row row)
    {
        long distance;
        if (row == null) {
            distance = Long.MAX_VALUE;
        } else {
            long z = row.value(zPosition).getInt64();
            distance = abs(z - zStart);
        }
        return distance;
    }

    // Object state

    // Iteration control is slightly convoluted. lt/geNeedToAdvance are set to indicate if the lt/geCursor
    // need to be advanced on the *next* iteration. This simplifies row management. It would be simpler to advance
    // in next() after determining the next row. But then the state carrying next gets advanced and next doesn't
    // have the state that it should.

    private final int zPosition;
    private final IterationHelper iterationHelper;
    private long zStart;
    private BindingsAwareCursor geCursor;
    private Row geRow;
    private long geDistance;
    private boolean geNeedToAdvance;
    private BindingsAwareCursor ltCursor;
    private Row ltRow;
    private long ltDistance;
    private boolean ltNeedToAdvance;
}
