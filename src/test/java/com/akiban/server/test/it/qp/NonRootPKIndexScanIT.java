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

package com.akiban.server.test.it.qp;

import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.test.ExpressionGenerators;
import org.junit.Before;
import org.junit.Test;

import static com.akiban.qp.operator.API.cursor;
import static com.akiban.qp.operator.API.indexScan_Default;

// Inspired by bug 1033754.

public class NonRootPKIndexScanIT extends OperatorITBase
{
    @Before
    public void before()
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
        schema = new Schema(rowDefCache().ais());
        parentRowType = schema.userTableRowType(userTable(parent));
        childRowType = schema.userTableRowType(userTable(child));
        childPKRowType = indexType(child, "cid");
        db = new NewRow[] {
            createNewRow(parent, 1L, 1L),
            createNewRow(child, 11L, 1L, 1L),
            createNewRow(child, 12L, 1L, 1L),
            createNewRow(parent, 2L),
            createNewRow(child, 21L, 2L, 2L),
            createNewRow(child, 22L, 2L, 2L),
        };
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
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
                RowBase[] expected = new RowBase[] {
                    row(childRowType, childPK, parentPK, parentPK),
                };
                compareRows(expected, cursor(plan, queryContext));
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
                RowBase[] expected = new RowBase[] {
                };
                compareRows(expected, cursor(plan, queryContext));
            }
            // Empty due to pid2
            for (long childPK : childPKs) {
                long parentPK = childPK / 10;
                IndexBound loBound = new IndexBound(row(childPKRowType, childPK, parentPK, parentPK + 1), SELECTOR);
                IndexBound hiBound = new IndexBound(row(childPKRowType, childPK, parentPK, parentPK + 1), SELECTOR);
                IndexKeyRange range = IndexKeyRange.bounded(childPKRowType, loBound, true, hiBound, true);
                Operator plan = indexScan_Default(childPKRowType, range, ordering(orderingMask));
                RowBase[] expected = new RowBase[] {
                };
                compareRows(expected, cursor(plan, queryContext));
            }
            // Non-empty
            for (long childPK : childPKs) {
                long parentPK = childPK / 10;
                IndexBound loBound = new IndexBound(row(childPKRowType, childPK, parentPK, parentPK), SELECTOR);
                IndexBound hiBound = new IndexBound(row(childPKRowType, childPK, parentPK, parentPK + 1), SELECTOR);
                IndexKeyRange range = IndexKeyRange.bounded(childPKRowType, loBound, true, hiBound, true);
                Operator plan = indexScan_Default(childPKRowType, range, ordering(orderingMask));
                RowBase[] expected = new RowBase[] {
                    row(childRowType, childPK, parentPK, parentPK),
                };
                compareRows(expected, cursor(plan, queryContext));
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
                RowBase[] expected = new RowBase[] {
                    row(childRowType, childPK, parentPK, parentPK),
                };
                compareRows(expected, cursor(plan, queryContext));
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
                RowBase[] expected = new RowBase[] {
                    row(childRowType, childPK, parentPK, parentPK),
                };
                compareRows(expected, cursor(plan, queryContext));
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
                RowBase[] expected = new RowBase[] {
                };
                compareRows(expected, cursor(plan, queryContext));
            }
            // Empty due to pid2
            for (long childPK : childPKs) {
                long parentPK = childPK / 10;
                IndexBound loBound = new IndexBound(row(childPKRowType, childPK, parentPK, parentPK + 1), SELECTOR);
                IndexBound hiBound = new IndexBound(row(childPKRowType, childPK, parentPK, parentPK + 1), SELECTOR);
                IndexKeyRange range = IndexKeyRange.bounded(childPKRowType, loBound, true, hiBound, true);
                Operator plan = indexScan_Default(childPKRowType, range, ordering(orderingMask));
                RowBase[] expected = new RowBase[] {
                };
                compareRows(expected, cursor(plan, queryContext));
            }
                // Non-empty
            for (long childPK : childPKs) {
                long parentPK = childPK / 10;
                IndexBound loBound = new IndexBound(row(childPKRowType, childPK, parentPK, parentPK), SELECTOR);
                IndexBound hiBound = new IndexBound(row(childPKRowType, childPK, parentPK, parentPK + 1), SELECTOR);
                IndexKeyRange range = IndexKeyRange.bounded(childPKRowType, loBound, true, hiBound, true);
                Operator plan = indexScan_Default(childPKRowType, range, ordering(orderingMask));
                RowBase[] expected = new RowBase[] {
                    row(childRowType, childPK, parentPK, parentPK),
                };
                compareRows(expected, cursor(plan, queryContext));
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
                RowBase[] expected = new RowBase[] {
                    row(childRowType, childPK, parentPK, parentPK),
                };
                compareRows(expected, cursor(plan, queryContext));
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
