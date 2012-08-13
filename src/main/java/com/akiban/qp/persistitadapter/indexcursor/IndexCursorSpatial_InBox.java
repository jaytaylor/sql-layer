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
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.expression.std.Expressions;
import com.akiban.server.geophile.BoxLatLon;
import com.akiban.server.geophile.SpaceLatLon;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;

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
        API.Ordering zOrdering = new API.Ordering();
        zOrdering.append(Expressions.field(keyRange.indexRowType().physicalRowType(), 0), true);
        for (IndexKeyRange zKeyRange : zKeyRanges(context, keyRange)) {
            IndexScanRowState rowState = new IndexScanRowState(adapter, keyRange.indexRowType());
            IndexCursorUnidirectional<ValueSource> zIntervalCursor =
                new IndexCursorUnidirectional<ValueSource>(context,
                                                           rowState,
                                                           zKeyRange,
                                                           zOrdering,
                                                           OldExpressionsSortKeyAdapter.INSTANCE);
            multiCursor.addCursor(zIntervalCursor);
        }
    }

    private static List<IndexKeyRange> zKeyRanges(QueryContext context, IndexKeyRange keyRange)
    {
        List<IndexKeyRange> zKeyRanges = new ArrayList<IndexKeyRange>();
        // TODO: Wraparound
        Index index = keyRange.indexRowType().index();
        IndexBound loBound = keyRange.lo();
        IndexBound hiBound = keyRange.hi();
        BoundExpressions loExpressions = loBound.boundExpressions(context);
        BoundExpressions hiExpressions = hiBound.boundExpressions(context);
        SpaceLatLon space = (SpaceLatLon) ((TableIndex)index).space();
        // Only 2d, lat/lon supported for now
        BigDecimal xLo = loExpressions.eval(0).getDecimal();
        BigDecimal xHi = hiExpressions.eval(0).getDecimal();
        BigDecimal yLo = loExpressions.eval(1).getDecimal();
        BigDecimal yHi = hiExpressions.eval(1).getDecimal();
        BoxLatLon box = new BoxLatLon(xLo, xHi, yLo, yHi);
        long[] zValues = new long[SpaceLatLon.MAX_DECOMPOSITION_Z_VALUES];
        space.decompose(box, zValues);
        for (int i = 0; i < zValues.length; i++) {
            long z = zValues[i];
            if (z != -1L) {
                IndexRowType physicalRowType = keyRange.indexRowType().physicalRowType();
                // lo bound of z
                ValuesHolderRow zLoRow = new ValuesHolderRow(physicalRowType, false);
                zLoRow.holderAt(0).expectType(AkType.LONG);
                zLoRow.holderAt(0).putLong(space.zLo(z));
                IndexBound zLo = new IndexBound(zLoRow, Z_SELECTOR);
                // hi bound of z
                ValuesHolderRow zHiRow = new ValuesHolderRow(physicalRowType, false);
                zHiRow.holderAt(0).expectType(AkType.LONG);
                zHiRow.holderAt(0).putLong(space.zHi(z));
                IndexBound zHi = new IndexBound(zHiRow, Z_SELECTOR);
                IndexKeyRange zKeyRange = IndexKeyRange.bounded(physicalRowType, zLo, true, zHi, true);
                zKeyRanges.add(zKeyRange);
            }
        }
        return zKeyRanges;
    }

    // Class state

    private static final ColumnSelector Z_SELECTOR = new SetColumnSelector(0);

    // Object state

    private final MultiCursor multiCursor;
    private final IterationHelper iterationHelper;
}
