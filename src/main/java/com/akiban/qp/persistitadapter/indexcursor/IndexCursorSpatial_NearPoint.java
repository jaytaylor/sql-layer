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

import com.akiban.ais.model.Index;
import com.akiban.qp.expression.BoundExpressions;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.persistitadapter.IndexScanRowState;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.server.api.dml.IndexRowPrefixSelector;
import com.akiban.server.expression.std.Expressions;
import com.akiban.server.geophile.SpaceLatLon;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.Types3Switch;
import com.akiban.server.types3.mcompat.mtypes.MBigDecimal;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;

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
        // This iteration uses the Exchanges in the IndexScanRowStates owned by each cursor of the MultiCursor.
        iterationHelper.closeIteration();
        geCursor.open();
        geRow = geCursor.next();
        geDistance = distanceFromStart(geRow);
        geNeedToAdvance = false;
        ltCursor.open();
        ltRow = ltCursor.next();
        ltDistance = distanceFromStart(ltRow);
        ltNeedToAdvance = false;
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
        } else if (ltNeedToAdvance) {
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
            close();
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
    public void destroy()
    {
        if (!isDestroyed()) {
            super.destroy();
            geCursor.destroy();
            ltCursor.destroy();
        }
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
        SpaceLatLon space = (SpaceLatLon) index.space();
        int latColumn = index.firstSpatialArgument();
        int lonColumn = latColumn + 1;
        // The index column selector needs to select all the columns before the z column, and the z column itself.
        IndexRowPrefixSelector indexColumnSelector = new IndexRowPrefixSelector(latColumn + 1);
        IndexBound loBound = keyRange.lo();
        BoundExpressions loExpressions = loBound.boundExpressions(context);
        // Compute z-value at beginning of forward and backward scans
        BigDecimal lat;
        BigDecimal lon;
        if (Types3Switch.ON) {
            TInstance latInstance = index.getAllColumns().get(latColumn).getColumn().tInstance();
            TInstance lonInstance = index.getAllColumns().get(lonColumn).getColumn().tInstance();
            lat = MBigDecimal.getWrapper(loExpressions.pvalue(latColumn), latInstance).asBigDecimal();
            lon = MBigDecimal.getWrapper(loExpressions.pvalue(lonColumn), lonInstance).asBigDecimal();
        } else {
            lat = loExpressions.eval(latColumn).getDecimal();
            lon = loExpressions.eval(lonColumn).getDecimal();
        }
        zStart = space.shuffle(lat, lon);
        // Cursors going forward from starting z value (inclusive), and backward from the same z value (exclusive)
        int indexRowFields = physicalIndexRowType.nFields();
        SpatialIndexBoundExpressions zForwardRow = new SpatialIndexBoundExpressions(indexRowFields);
        SpatialIndexBoundExpressions zBackwardRow = new SpatialIndexBoundExpressions(indexRowFields);
        SpatialIndexBoundExpressions zMaxRow = new SpatialIndexBoundExpressions(indexRowFields);
        SpatialIndexBoundExpressions zMinRow = new SpatialIndexBoundExpressions(indexRowFields);
        IndexBound zForward = new IndexBound(zForwardRow, indexColumnSelector);
        IndexBound zBackward = new IndexBound(zBackwardRow, indexColumnSelector);
        IndexBound zMax = new IndexBound(zMaxRow, indexColumnSelector);
        IndexBound zMin = new IndexBound(zMinRow, indexColumnSelector);
        // Take care of any equality restrictions before the spatial fields
        zPosition = latColumn;
        for (int f = 0; f < zPosition; f++) {
            if (Types3Switch.ON) {
                PValueSource eqValueSource = loExpressions.pvalue(f);
                zForwardRow.value(f, eqValueSource);
                zBackwardRow.value(f, eqValueSource);
                zMaxRow.value(f, eqValueSource);
                zMinRow.value(f, eqValueSource);
            } else {
                ValueSource eqValue = loExpressions.eval(f);
                zForwardRow.value(f, eqValue);
                zBackwardRow.value(f, eqValue);
                zMaxRow.value(f, eqValue);
                zMinRow.value(f, eqValue);
            }
        }
        // Z-value part of bounds
        if (Types3Switch.ON) {
            PValue startPValue = new PValue(MNumeric.BIGINT.instance(false));
            PValue maxPValue = new PValue(MNumeric.BIGINT.instance(false));
            PValue minPValue = new PValue(MNumeric.BIGINT.instance(false));
            startPValue.putInt64(zStart);
            maxPValue.putInt64(Long.MAX_VALUE);
            minPValue.putInt64(Long.MIN_VALUE);
            zForwardRow.value(zPosition, startPValue);
            zBackwardRow.value(zPosition, startPValue);
            zMaxRow.value(zPosition, maxPValue);
            zMinRow.value(zPosition, minPValue);
        } else {
            ValueSource startValue = new ValueHolder(AkType.LONG, zStart);
            ValueSource maxValue = new ValueHolder(AkType.LONG, Long.MAX_VALUE);
            ValueSource minValue = new ValueHolder(AkType.LONG, Long.MIN_VALUE);
            zForwardRow.value(zPosition, startValue);
            zBackwardRow.value(zPosition, startValue);
            zMaxRow.value(zPosition, maxValue);
            zMinRow.value(zPosition, minValue);
        }
        IndexKeyRange geKeyRange = IndexKeyRange.bounded(physicalIndexRowType, zForward, true, zMax, false);
        IndexKeyRange ltKeyRange = IndexKeyRange.bounded(physicalIndexRowType, zMin, false, zBackward, false);
        IndexScanRowState geRowState = new IndexScanRowState(adapter, keyRange.indexRowType());
        IndexScanRowState ltRowState = new IndexScanRowState(adapter, keyRange.indexRowType());
        API.Ordering upOrdering = new API.Ordering();
        API.Ordering downOrdering = new API.Ordering();
        for (int f = 0; f < physicalIndexRowType.nFields(); f++) {
            upOrdering.append(Expressions.field(physicalIndexRowType, 0), true);
            downOrdering.append(Expressions.field(physicalIndexRowType, 0), false);
        }
        if (Types3Switch.ON) {
            geCursor = new IndexCursorUnidirectional<PValueSource>(context,
                                                                   geRowState,
                                                                   geKeyRange,
                                                                   upOrdering,
                                                                   PValueSortKeyAdapter.INSTANCE);
            ltCursor = new IndexCursorUnidirectional<PValueSource>(context,
                                                                   ltRowState,
                                                                   ltKeyRange,
                                                                   downOrdering,
                                                                   PValueSortKeyAdapter.INSTANCE);
        }
        else {
            geCursor = new IndexCursorUnidirectional<ValueSource>(context,
                                                                  geRowState,
                                                                  geKeyRange,
                                                                  upOrdering,
                                                                  OldExpressionsSortKeyAdapter.INSTANCE);
            ltCursor = new IndexCursorUnidirectional<ValueSource>(context,
                                                                  ltRowState,
                                                                  ltKeyRange,
                                                                  downOrdering,
                                                                  OldExpressionsSortKeyAdapter.INSTANCE);
        }
    }

    // For use by this class

    private long distanceFromStart(Row row)
    {
        long distance;
        if (row == null) {
            distance = Long.MAX_VALUE;
        } else if (Types3Switch.ON) {
            long z = row.pvalue(zPosition).getInt64();
            distance = abs(z - zStart);
        } else {
            long z = row.eval(zPosition).getLong();
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
    private Cursor geCursor;
    private Row geRow;
    private long geDistance;
    private boolean geNeedToAdvance;
    private Cursor ltCursor;
    private Row ltRow;
    private long ltDistance;
    private boolean ltNeedToAdvance;
}
