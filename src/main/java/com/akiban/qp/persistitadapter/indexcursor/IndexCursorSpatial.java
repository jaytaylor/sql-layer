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
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.ValuesHolderRow;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.util.MultiCursor;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.expression.std.Expressions;
import com.akiban.server.geophile.Box2;
import com.akiban.server.geophile.Space;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;

import java.util.ArrayList;
import java.util.List;

// A scan of an IndexCursorSpatial will be implemented as one or more IndexCursorUnidirectional scans.

class IndexCursorSpatial extends IndexCursor
{
    @Override
    public void open()
    {
        super.open();
        // iterationHelper.closeIteration() closes the PersistitIndexCursor, releasing its Exchange.
        // iterationHelper.reopen() will reopen the PIC, getting a new Exchange for each constituent
        // Cursor of multiCursor. (For more information, see the comment below on ReopeningCursor.)
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
    public void jump(Row row, ColumnSelector columnSelector)
    {
        throw new UnsupportedOperationException();
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
    public boolean isIdle()
    {
        return super.isIdle();
    }

    @Override
    public boolean isActive()
    {
        return super.isActive();
    }

    @Override
    public boolean isDestroyed()
    {
        return super.isDestroyed();
    }

    // IndexCursorSpatial interface

    public static IndexCursorSpatial create(QueryContext context,
                                            IterationHelper iterationHelper,
                                            IndexKeyRange keyRange)
    {
        return new IndexCursorSpatial(context, iterationHelper, keyRange);
    }

    // For use by this class

    private IndexCursorSpatial(QueryContext context, IterationHelper iterationHelper, IndexKeyRange keyRange)
    {
        super(context, iterationHelper);
        assert keyRange.spatial();
        this.multiCursor = new MultiCursor();
        this.iterationHelper = iterationHelper;
        API.Ordering zOrdering = new API.Ordering();
        zOrdering.append(Expressions.field(keyRange.indexRowType().physicalRowType(), 0), true);
        for (IndexKeyRange zKeyRange : zKeyRanges(context, keyRange)) {
            IndexCursorUnidirectional<ValueSource> zIntervalCursor =
                new IndexCursorUnidirectional<ValueSource>(context,
                                                           iterationHelper,
                                                           zKeyRange,
                                                           zOrdering,
                                                           OldExpressionsSortKeyAdapter.INSTANCE);
            multiCursor.addCursor(new ReopeningCursor(zIntervalCursor, iterationHelper));
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
                // lo bound of z
                ValuesHolderRow zLoRow = new ValuesHolderRow(physicalRowType, false);
                zLoRow.holderAt(0).expectType(AkType.LONG);
                zLoRow.holderAt(0).putLong(space.zLo(z));
                IndexBound zLo = new IndexBound(zLoRow, new SetColumnSelector(0));
                // hi bound of z
                ValuesHolderRow zHiRow = new ValuesHolderRow(physicalRowType, false);
                zHiRow.holderAt(0).expectType(AkType.LONG);
                zHiRow.holderAt(0).putLong(space.zHi(z));
                IndexBound zHi = new IndexBound(zHiRow, new SetColumnSelector(0));
                IndexKeyRange zKeyRange = IndexKeyRange.bounded(physicalRowType, zLo, true, zHi, true);
                zKeyRanges.add(zKeyRange);
            }
        }
        return zKeyRanges;
    }

    // Object state

    private final MultiCursor multiCursor;
    private final IterationHelper iterationHelper;

    // Inner classes

    // Why ReopeningCursor is needed: An IndexCursorSpatial is wrapped by a PersistitIndexCursor. The PICs
    // IterationHelper provides access to the PICs Exchange and current row to the IndexCursorUnidirectionals
    // managed by the MultiCursor. Closing an ICU results in the PIC being closed, which returns the Exchange.
    // Need to reopen via the IterationHelper to get a new exchange.

    private static class ReopeningCursor implements Cursor
    {
        // Cursor interface

        @Override
        public void open()
        {
            iterationHelper.reopenIteration();
            input.open();
        }

        @Override
        public Row next()
        {
            return input.next();
        }

        @Override
        public void jump(Row row, ColumnSelector columnSelector)
        {
            input.jump(row, columnSelector);
        }

        @Override
        public void close()
        {
            input.close();
        }

        @Override
        public void destroy()
        {
            input.destroy();
        }

        @Override
        public boolean isIdle()
        {
            return input.isIdle();
        }

        @Override
        public boolean isActive()
        {
            return input.isActive();
        }

        @Override
        public boolean isDestroyed()
        {
            return input.isDestroyed();
        }

        // ReopeningCursor interface

        public ReopeningCursor(Cursor input, IterationHelper iterationHelper)
        {
            this.input = input;
            this.iterationHelper = iterationHelper;
        }

        // Object state

        private final Cursor input;
        private final IterationHelper iterationHelper;
    }
}
