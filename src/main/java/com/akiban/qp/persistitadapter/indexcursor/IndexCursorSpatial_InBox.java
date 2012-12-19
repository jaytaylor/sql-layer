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
import com.akiban.ais.model.TableIndex;
import com.akiban.qp.expression.BoundExpressions;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.persistitadapter.IndexScanRowState;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.ValuesHolderRow;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.util.MultiCursor;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.api.dml.IndexRowPrefixSelector;
import com.akiban.server.expression.std.Expressions;
import com.akiban.server.geophile.BoxLatLon;
import com.akiban.server.geophile.Space;
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
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValueTargets;
import com.akiban.server.types3.texpressions.TPreparedExpression;
import com.akiban.server.types3.texpressions.TPreparedField;

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

    // IndexCursorSpatial_InBox interface

    public static IndexCursorSpatial_InBox create(QueryContext context,
                                                  IterationHelper iterationHelper,
                                                  IndexKeyRange keyRange)
    {
        return  new IndexCursorSpatial_InBox(context, iterationHelper, keyRange);
    }

    // For use by this class

    private IndexCursorSpatial_InBox(QueryContext context, IterationHelper iterationHelper, IndexKeyRange keyRange)
    {
        super(context, iterationHelper);
        assert keyRange.spatial();
        this.multiCursor = new MultiCursor();
        this.iterationHelper = iterationHelper;
        Index spatialIndex = keyRange.indexRowType().index();
        assert spatialIndex.isSpatial() : spatialIndex;
        this.space = spatialIndex.space();
        this.latColumn = spatialIndex.firstSpatialArgument();
        this.lonColumn = latColumn + 1;
        API.Ordering zOrdering = new API.Ordering();
        IndexRowType rowType = keyRange.indexRowType().physicalRowType();
        for (int f = 0; f < rowType.nFields(); f++) {
            if (Types3Switch.ON) {
                zOrdering.append(null, new TPreparedField(rowType.typeInstanceAt(f), f), true);
            } else {
                zOrdering.append(Expressions.field(rowType, f), null, true);
            }
        }
        // The index column selector needs to select all the columns before the z column, and the z column itself.
        this.indexColumnSelector = new IndexRowPrefixSelector(this.latColumn + 1);
        for (IndexKeyRange zKeyRange : zKeyRanges(context, keyRange)) {
            IndexScanRowState rowState = new IndexScanRowState(adapter, keyRange.indexRowType());
            if (Types3Switch.ON) {
                IndexCursorUnidirectional<PValueSource> zIntervalCursor =
                    new IndexCursorUnidirectional<PValueSource>(context,
                                                                rowState,
                                                                zKeyRange,
                                                                zOrdering,
                                                                PValueSortKeyAdapter.INSTANCE);
                multiCursor.addCursor(zIntervalCursor);
            }
            else {
                IndexCursorUnidirectional<ValueSource> zIntervalCursor =
                    new IndexCursorUnidirectional<ValueSource>(context,
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
        List<IndexKeyRange> zKeyRanges = new ArrayList<IndexKeyRange>();
        Index index = keyRange.indexRowType().index();
        IndexBound loBound = keyRange.lo();
        IndexBound hiBound = keyRange.hi();
        BoundExpressions loExpressions = loBound.boundExpressions(context);
        BoundExpressions hiExpressions = hiBound.boundExpressions(context);
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
                    PValue loPValue = new PValue(MNumeric.BIGINT);
                    PValue hiPValue = new PValue(MNumeric.BIGINT);
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
