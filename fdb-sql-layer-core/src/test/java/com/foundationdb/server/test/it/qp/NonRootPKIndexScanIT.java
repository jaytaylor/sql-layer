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

package com.foundationdb.server.test.it.qp;

import com.foundationdb.qp.expression.IndexBound;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.server.api.dml.SetColumnSelector;
import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.test.ExpressionGenerators;
import org.junit.Test;

import static com.foundationdb.qp.operator.API.cursor;
import static com.foundationdb.qp.operator.API.indexScan_Default;

// Inspired by bug 1033754.

public class NonRootPKIndexScanIT extends OperatorITBase
{
    @Override
    protected void setupCreateSchema()
    {
        parent = createTable(
            "schema", "parent",
            "pid1 int not null",
            "pid2 int not null",
            "primary key(pid1, pid2)");
        child = createTable(
            "schema", "child",
            "cid int not null",
            "pid1 int",
            "pid2 int",
            "primary key(cid)",
            "grouping foreign key (pid1, pid2) references parent(pid1, pid2)");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        schema = new Schema(ais());
        parentRowType = schema.tableRowType(table(parent));
        childRowType = schema.tableRowType(table(child));
        childPKRowType = indexType(child, "cid");
        db = new NewRow[] {
            createNewRow(parent, 1L, 1L),
            createNewRow(child, 11L, 1L, 1L),
            createNewRow(child, 12L, 1L, 1L),
            createNewRow(parent, 2L, 2L),
            createNewRow(child, 21L, 2L, 2L),
            createNewRow(child, 22L, 2L, 2L),
        };
        adapter = newStoreAdapter(schema);
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
        use(db);
    }

    @Test
    public void testAtChildAtParentUnidirectional()
    {
        long[] childPKs = new long[] {11, 12, 21, 22};
        int[] orderingMasks = new int[]{ALL_ASCENDING, ALL_DESCENDING};
        for (int orderingMask : orderingMasks) {
            for (long childPK : childPKs) {
                long parentPK = childPK / 10;
                IndexBound loBound = new IndexBound(row(childPKRowType, childPK, parentPK, parentPK), SELECTOR);
                IndexBound hiBound = new IndexBound(row(childPKRowType, childPK, parentPK, parentPK), SELECTOR);
                IndexKeyRange range = IndexKeyRange.bounded(childPKRowType, loBound, true, hiBound, true);
                Operator plan = indexScan_Default(childPKRowType, range, ordering(orderingMask));
                Row[] expected = new Row[] {
                    row(childRowType, childPK, parentPK, parentPK),
                };
                compareRows(expected, cursor(plan, queryContext, queryBindings));
            }
        }
    }

    @Test
    public void testAtChildAfterParentUnidirectional()
    {
        long[] childPKs = new long[] {11, 12, 21, 22};
        int[] orderingMasks = new int[]{ALL_ASCENDING, ALL_DESCENDING};
        for (int orderingMask : orderingMasks) {
            // Empty due to pid1
            for (long childPK : childPKs) {
                long parentPK = childPK / 10;
                IndexBound loBound = new IndexBound(row(childPKRowType, childPK, parentPK + 1, parentPK + 1), SELECTOR);
                IndexBound hiBound = new IndexBound(row(childPKRowType, childPK, parentPK + 1, parentPK + 1), SELECTOR);
                IndexKeyRange range = IndexKeyRange.bounded(childPKRowType, loBound, true, hiBound, true);
                Operator plan = indexScan_Default(childPKRowType, range, ordering(orderingMask));
                Row[] expected = new Row[] {
                };
                compareRows(expected, cursor(plan, queryContext, queryBindings));
            }
            // Empty due to pid2
            for (long childPK : childPKs) {
                long parentPK = childPK / 10;
                IndexBound loBound = new IndexBound(row(childPKRowType, childPK, parentPK, parentPK + 1), SELECTOR);
                IndexBound hiBound = new IndexBound(row(childPKRowType, childPK, parentPK, parentPK + 1), SELECTOR);
                IndexKeyRange range = IndexKeyRange.bounded(childPKRowType, loBound, true, hiBound, true);
                Operator plan = indexScan_Default(childPKRowType, range, ordering(orderingMask));
                Row[] expected = new Row[] {
                };
                compareRows(expected, cursor(plan, queryContext, queryBindings));
            }
            // Non-empty
            for (long childPK : childPKs) {
                long parentPK = childPK / 10;
                IndexBound loBound = new IndexBound(row(childPKRowType, childPK, parentPK, parentPK), SELECTOR);
                IndexBound hiBound = new IndexBound(row(childPKRowType, childPK, parentPK, parentPK + 1), SELECTOR);
                IndexKeyRange range = IndexKeyRange.bounded(childPKRowType, loBound, true, hiBound, true);
                Operator plan = indexScan_Default(childPKRowType, range, ordering(orderingMask));
                Row[] expected = new Row[] {
                    row(childRowType, childPK, parentPK, parentPK),
                };
                compareRows(expected, cursor(plan, queryContext, queryBindings));
            }
        }
    }

    @Test
    public void testAtChildBeforeParentUnidirectional()
    {
        long[] childPKs = new long[] {11, 12, 21, 22};
        int[] orderingMasks = new int[]{ALL_ASCENDING, ALL_DESCENDING};
        for (int orderingMask : orderingMasks) {
            for (long childPK : childPKs) {
                long parentPK = childPK / 10;
                IndexBound loBound = new IndexBound(row(childPKRowType, childPK, parentPK, parentPK - 1), SELECTOR);
                IndexBound hiBound = new IndexBound(row(childPKRowType, childPK, parentPK, parentPK), SELECTOR);
                IndexKeyRange range = IndexKeyRange.bounded(childPKRowType, loBound, true, hiBound, true);
                Operator plan = indexScan_Default(childPKRowType, range, ordering(orderingMask));
                Row[] expected = new Row[] {
                    row(childRowType, childPK, parentPK, parentPK),
                };
                compareRows(expected, cursor(plan, queryContext, queryBindings));
            }
        }
    }

    @Test
    public void testAtChildAtParentMixedOrder()
    {
        long[] childPKs = new long[] {11, 12, 21, 22};
        for (int orderingMask = 0x0; orderingMask <= 0x7; orderingMask++) {
            for (long childPK : childPKs) {
                long parentPK = childPK / 10;
                IndexBound bound = new IndexBound(row(childPKRowType, childPK, parentPK, parentPK), SELECTOR);
                IndexKeyRange range = IndexKeyRange.bounded(childPKRowType, bound, true, bound, true);
                Operator plan = indexScan_Default(childPKRowType, range, ordering(orderingMask));
                Row[] expected = new Row[] {
                    row(childRowType, childPK, parentPK, parentPK),
                };
                compareRows(expected, cursor(plan, queryContext, queryBindings));
            }
        }
    }

    @Test
    public void testAtChildAfterParentMixedOrder()
    {
        long[] childPKs = new long[] {11, 12, 21, 22};
        for (int orderingMask = 0x0; orderingMask <= 0x7; orderingMask++) {
            // Empty due to pid1
            for (long childPK : childPKs) {
                long parentPK = childPK / 10;
                IndexBound loBound = new IndexBound(row(childPKRowType, childPK, parentPK + 1, parentPK + 1), SELECTOR);
                IndexBound hiBound = new IndexBound(row(childPKRowType, childPK, parentPK + 1, parentPK + 1), SELECTOR);
                IndexKeyRange range = IndexKeyRange.bounded(childPKRowType, loBound, true, hiBound, true);
                Operator plan = indexScan_Default(childPKRowType, range, ordering(orderingMask));
                Row[] expected = new Row[] {
                };
                compareRows(expected, cursor(plan, queryContext, queryBindings));
            }
            // Empty due to pid2
            for (long childPK : childPKs) {
                long parentPK = childPK / 10;
                IndexBound loBound = new IndexBound(row(childPKRowType, childPK, parentPK, parentPK + 1), SELECTOR);
                IndexBound hiBound = new IndexBound(row(childPKRowType, childPK, parentPK, parentPK + 1), SELECTOR);
                IndexKeyRange range = IndexKeyRange.bounded(childPKRowType, loBound, true, hiBound, true);
                Operator plan = indexScan_Default(childPKRowType, range, ordering(orderingMask));
                Row[] expected = new Row[] {
                };
                compareRows(expected, cursor(plan, queryContext, queryBindings));
            }
                // Non-empty
            for (long childPK : childPKs) {
                long parentPK = childPK / 10;
                IndexBound loBound = new IndexBound(row(childPKRowType, childPK, parentPK, parentPK), SELECTOR);
                IndexBound hiBound = new IndexBound(row(childPKRowType, childPK, parentPK, parentPK + 1), SELECTOR);
                IndexKeyRange range = IndexKeyRange.bounded(childPKRowType, loBound, true, hiBound, true);
                Operator plan = indexScan_Default(childPKRowType, range, ordering(orderingMask));
                Row[] expected = new Row[] {
                    row(childRowType, childPK, parentPK, parentPK),
                };
                compareRows(expected, cursor(plan, queryContext, queryBindings));
            }
        }
    }

    @Test
    public void testAtChildBeforeParentMixedOrder()
    {
        long[] childPKs = new long[] {11, 12, 21, 22};
        for (int orderingMask = 0x0; orderingMask <= 0x7; orderingMask++) {
            for (long childPK : childPKs) {
                long parentPK = childPK / 10;
                IndexBound loBound = new IndexBound(row(childPKRowType, childPK, parentPK, parentPK - 1), SELECTOR);
                IndexBound hiBound = new IndexBound(row(childPKRowType, childPK, parentPK, parentPK), SELECTOR);
                IndexKeyRange range = IndexKeyRange.bounded(childPKRowType, loBound, true, hiBound, true);
                Operator plan = indexScan_Default(childPKRowType, range, ordering(orderingMask));
                Row[] expected = new Row[] {
                    row(childRowType, childPK, parentPK, parentPK),
                };
                compareRows(expected, cursor(plan, queryContext, queryBindings));
            }
        }
    }

    private API.Ordering ordering(int mask)
    {
        API.Ordering ordering = new API.Ordering();
        ordering.append(ExpressionGenerators.field(childPKRowType, 0), (mask & 0x1) != 0);
        ordering.append(ExpressionGenerators.field(childPKRowType, 1), (mask & 0x2) != 0);
        ordering.append(ExpressionGenerators.field(childPKRowType, 2), (mask & 0x4) != 0);
        return ordering;
    }

    private static final int ALL_ASCENDING = 0x7;
    private static final int ALL_DESCENDING = 0x0;
    private static final ColumnSelector SELECTOR = new SetColumnSelector(0, 1, 2);

    private int parent;
    private int child;
    private RowType parentRowType;
    private RowType childRowType;
    private IndexRowType childPKRowType;
}
