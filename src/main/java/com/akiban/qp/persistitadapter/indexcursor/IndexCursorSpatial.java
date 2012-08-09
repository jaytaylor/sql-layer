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
import com.akiban.qp.row.ValuesHolderRow;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.geophile.Box2;
import com.akiban.server.geophile.Space;
import com.akiban.server.types.AkType;
import com.persistit.Key;

import java.util.ArrayList;
import java.util.List;

// For a z-order scan. A single IndexKeyRange will usually result in multiple scans

class IndexCursorSpatial<S> extends IndexCursorUnidirectional<S>
{
    // IndexCursorUnidirectional interface

    public static <S> IndexCursorSpatial<S> create(QueryContext context,
                                                   IterationHelper iterationHelper,
                                                   IndexKeyRange keyRange,
                                                   SortKeyAdapter<S, ?> sortKeyAdapter)
    {
        assert keyRange.spatial();
        // We get here while opening an IndexScan_Default cursor, so any IndexKeyRange variables are bound.
        // (The parent class as written before this was true, so it delays evaluation. We're relying on that
        // class so evaluation will be repeated.)
        List<IndexKeyRange> zKeyRanges = zKeyRanges(context, keyRange);
        return new IndexCursorSpatial<S>(context, iterationHelper, keyRange, zAscending(), sortKeyAdapter);
    }

    @Override
    protected void evaluateBoundaries(QueryContext context, SortKeyAdapter<S, ?> keyAdapter)
    {
        BoundExpressions startExpressions = null;
        if (startBoundColumns == 0 || start == null) {
            startKey.append(startBoundary);
        } else {
            startExpressions = start.boundExpressions(context);
            /* TODO: Obsolete
            startKey.clear();
            startKeyTarget.attach(startKey);
            for (int f = 0; f < startBoundColumns; f++) {
                if (start.columnSelector().includesColumn(f)) {
                    S source = keyAdapter.get(startExpressions, f);
                    startKeyTarget.append(source, f, types, tInstances, collators);
                }
            }
            */
        }
        BoundExpressions endExpressions;
        if (endBoundColumns == 0 || end == null) {
            endKey = null;
        } else {
            endExpressions = end.boundExpressions(context);
            /* TODO: Obsolete
            endKey.clear();
            endKeyTarget.attach(endKey);
            for (int f = 0; f < endBoundColumns; f++) {
                if (end.columnSelector().includesColumn(f)) {
                    S source = keyAdapter.get(endExpressions, f);
                    if (keyAdapter.isNull(source) && startExpressions != null && !startExpressions.eval(f).isNull()) {
                        endKey.append(Key.AFTER);
                    } else {
                        endKeyTarget.append(source, f, types, tInstances, collators);
                    }
                } else {
                    endKey.append(Key.AFTER);
                }
            }
            */
        }
    }
    // For use by this class

    private IndexCursorSpatial(QueryContext context,
                               IterationHelper iterationHelper,
                               IndexKeyRange keyRange,
                               API.Ordering ordering,
                               SortKeyAdapter<S, ?> sortKeyAdapter)
    {
        super(context, iterationHelper, keyRange, ordering, sortKeyAdapter);
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
        Space space = ((TableIndex)index).space();
        // Only 2d supported for now
        long xLo = loExpressions.eval(0).getLong();
        long xHi = hiExpressions.eval(0).getLong();
        long yLo = loExpressions.eval(1).getLong();
        long yHi = hiExpressions.eval(1).getLong();
        Box2 box = new Box2(xLo, xHi, yLo, yHi);
        long[] zValues = new long[4];
        space.decompose(box, zValues);
        for (int i = 0; i < 4; i++) {
            long z = zValues[i];
            if (z != -1L) {
                IndexRowType physicalRowType = keyRange.indexRowType().physicalRowType();
                ValuesHolderRow zLoRow = new ValuesHolderRow(physicalRowType, false);
                ValuesHolderRow zHiRow = new ValuesHolderRow(physicalRowType, false);
                zLoRow.holderAt(0).expectType(AkType.LONG);
                zLoRow.holderAt(0).putLong(space.zLo(z));
                zHiRow.holderAt(0).expectType(AkType.LONG);
                zHiRow.holderAt(0).putLong(space.zHi(z));
                IndexBound zLo = new IndexBound(zLoRow, new SetColumnSelector(0));
                IndexBound zHi = new IndexBound(zHiRow, new SetColumnSelector(0));
                IndexKeyRange zKeyRange = IndexKeyRange.bounded(physicalRowType, zLo, true, zHi, true);
                zKeyRanges.add(zKeyRange);
            }
        }
        return zKeyRanges;
    }

    private static API.Ordering zAscending()
    {
        API.Ordering ordering = new API.Ordering();
        ordering.append(null, true);
        return ordering;
    }
}
