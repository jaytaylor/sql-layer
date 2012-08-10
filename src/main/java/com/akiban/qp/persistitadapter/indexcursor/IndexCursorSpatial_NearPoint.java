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

import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.expression.RowBasedUnboundExpressions;
import com.akiban.qp.expression.UnboundExpressions;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.persistitadapter.IndexScanRowState;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.expression.std.Expressions;
import com.akiban.server.types.ValueSource;

import java.util.Collections;

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
        // Cursor going forward from starting z value (inclusive)
        UnboundExpressions zMaxRow =
            new RowBasedUnboundExpressions(physicalIndexRowType,
                                           Collections.singletonList(Expressions.literal(Long.MAX_VALUE)));
        IndexBound zMax = new IndexBound(zMaxRow, Z_SELECTOR);
        IndexKeyRange geKeyRange = IndexKeyRange.bounded(physicalIndexRowType, keyRange.lo(), true, zMax, false);
        IndexScanRowState geRowState = new IndexScanRowState(adapter, keyRange.indexRowType());
        API.Ordering upOrdering = new API.Ordering();
        upOrdering.append(Expressions.field(physicalIndexRowType, 0), true);
        geCursor = new IndexCursorUnidirectional<ValueSource>(context,
                                                              geRowState,
                                                              geKeyRange,
                                                              upOrdering,
                                                              OldExpressionsSortKeyAdapter.INSTANCE);
        // Cursor going backward from starting z value (exclusive)
        UnboundExpressions zMinRow =
            new RowBasedUnboundExpressions(physicalIndexRowType,
                                           Collections.singletonList(Expressions.literal(Long.MIN_VALUE)));
        IndexBound zMin = new IndexBound(zMinRow, Z_SELECTOR);
        IndexKeyRange ltKeyRange = IndexKeyRange.bounded(physicalIndexRowType, zMin, false, keyRange.lo(), false);
        IndexScanRowState ltRowState = new IndexScanRowState(adapter, keyRange.indexRowType());
        API.Ordering downOrdering = new API.Ordering();
        downOrdering.append(Expressions.field(physicalIndexRowType, 0), false);
        ltCursor = new IndexCursorUnidirectional<ValueSource>(context,
                                                              ltRowState,
                                                              ltKeyRange,
                                                              downOrdering,
                                                              OldExpressionsSortKeyAdapter.INSTANCE);
        // Remember where we start, so that we can compute distance from this point.
        zStart = keyRange.lo().boundExpressions(context).eval(0).getLong();
    }

    // For use by this class

    private long distanceFromStart(Row row)
    {
        long distance;
        if (row == null) {
            distance = Long.MAX_VALUE;
        } else {
            long z = row.eval(0).getLong();
            distance = abs(z - zStart);
        }
        return distance;
    }

    // Class state

    private static final ColumnSelector Z_SELECTOR = new SetColumnSelector(0);

    // Object state

    // Iteration control is slightly convoluted. lt/geNeedToAdvance are set to indicate if the lt/geCursor
    // need to be advanced on the *next* iteration. This simplifies row management. It would be simpler to advance
    // in next() after determining the next row. But then the state carrying next gets advanced and next doesn't
    // have the state that it should.

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
