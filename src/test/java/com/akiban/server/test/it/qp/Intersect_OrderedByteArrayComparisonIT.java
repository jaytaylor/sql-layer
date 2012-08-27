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
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.expression.Expression;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.EnumSet;

import static com.akiban.qp.operator.API.*;
import static com.akiban.server.expression.std.Expressions.field;

// Testing Intersect_Ordered key comparisons, which are done at the Persistit level.

@Ignore
public class Intersect_OrderedByteArrayComparisonIT extends OperatorITBase
{
    @Before
    public void before()
    {
        t = createTable(
            "schema", "t",
            "id int not null",
            "test int", // test case
            // For left index scan
            "l1 int",
            "l2 varchar(10)",
            "l3 int",
            // For right index scan
            "r1 int",
            "r2 varchar(10)",
            "r3 int",
            "primary key(id)");
        createIndex("schema", "t", "idx_left", "test", "l1", "l2", "l3");
        createIndex("schema", "t", "idx_right", "test", "r1", "r2", "r3");
        schema = new Schema(rowDefCache().ais());
        tRowType = schema.userTableRowType(userTable(t));
        leftIndexRowType = indexType(t, "test", "l1", "l2", "l3");
        rightIndexRowType = indexType(t, "test", "r1", "r2", "r3");
        coi = group(t);
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
        db = new NewRow[]{
            // 1: Comparisons need only examine k1 on a mismatch.
            createNewRow(t, 1000, 1, 500, "x", 999, 501, "x", 999),
            createNewRow(t, 1001, 1, 502, "x", 999, 502, "x", 999),
            createNewRow(t, 1002, 1, 502, "x", 999, 502, "x", 999),
            createNewRow(t, 1003, 1, 504, "x", 999, 503, "x", 999),
            // 2: k1 always equal, k2s differ in length
            createNewRow(t, 1008, 2, 500, "x", 999, 500, "xx", 999),
            createNewRow(t, 1009, 2, 500, "xxx", 999, 500, "xxx", 999),
            createNewRow(t, 1010, 2, 500, "xxx", 999, 500, "xxx", 999),
            createNewRow(t, 1011, 2, 500, "xxxxx", 999, 500, "xxxx", 999),
            // 3: k1, k2 always match, difference is in k3
            createNewRow(t, 1016, 3, 500, "x", 900, 500, "x", 901),
            createNewRow(t, 1017, 3, 500, "x", 902, 500, "x", 902),
            createNewRow(t, 1018, 3, 500, "x", 902, 500, "x", 902),
            createNewRow(t, 1019, 3, 500, "x", 904, 500, "x", 903),
        };
        use(db);
    }

    @Test
    public void test1()
    {
        plan = intersectPlan(1, IntersectOption.OUTPUT_LEFT, true, false);
        expected = new RowBase[]{
            row(leftIndexRowType, 1L, 502L, "x", 999L, 1001L),
            row(leftIndexRowType, 1L, 502L, "x", 999L, 1002L),
        };
        dump(plan);
        compareRows(expected, cursor(plan, queryContext));
        plan = intersectPlan(1, IntersectOption.OUTPUT_RIGHT, true, false);
        expected = new RowBase[]{
            row(leftIndexRowType, 1L, 502L, "x", 999L, 1001L),
            row(leftIndexRowType, 1L, 502L, "x", 999L, 1002L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = intersectPlan(1, IntersectOption.OUTPUT_LEFT, false, false);
        expected = new RowBase[]{
            row(leftIndexRowType, 1L, 502L, "x", 999L, 1001L),
            row(leftIndexRowType, 1L, 502L, "x", 999L, 1002L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = intersectPlan(1, IntersectOption.OUTPUT_RIGHT, false, false);
        expected = new RowBase[]{
            row(leftIndexRowType, 1L, 502L, "x", 999L, 1001L),
            row(leftIndexRowType, 1L, 502L, "x", 999L, 1002L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = intersectPlan(1, IntersectOption.OUTPUT_LEFT, true, true);
        expected = new RowBase[]{
            row(leftIndexRowType, 1L, 502L, "x", 999L, 1001L),
            row(leftIndexRowType, 1L, 502L, "x", 999L, 1002L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = intersectPlan(1, IntersectOption.OUTPUT_RIGHT, true, true);
        expected = new RowBase[]{
            row(leftIndexRowType, 1L, 502L, "x", 999L, 1001L),
            row(leftIndexRowType, 1L, 502L, "x", 999L, 1002L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = intersectPlan(1, IntersectOption.OUTPUT_LEFT, false, true);
        expected = new RowBase[]{
            row(leftIndexRowType, 1L, 502L, "x", 999L, 1001L),
            row(leftIndexRowType, 1L, 502L, "x", 999L, 1002L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = intersectPlan(1, IntersectOption.OUTPUT_RIGHT, false, true);
        expected = new RowBase[]{
            row(leftIndexRowType, 1L, 502L, "x", 999L, 1001L),
            row(leftIndexRowType, 1L, 502L, "x", 999L, 1002L),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void test2()
    {
        plan = intersectPlan(2, IntersectOption.OUTPUT_LEFT, true, false);
        expected = new RowBase[]{
            row(leftIndexRowType, 2L, 500L, "xxx", 999L, 1009L),
            row(leftIndexRowType, 2L, 500L, "xxx", 999L, 1010L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = intersectPlan(2, IntersectOption.OUTPUT_RIGHT, true, false);
        expected = new RowBase[]{
            row(leftIndexRowType, 2L, 500L, "xxx", 999L, 1009L),
            row(leftIndexRowType, 2L, 500L, "xxx", 999L, 1010L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = intersectPlan(2, IntersectOption.OUTPUT_LEFT, false, false);
        expected = new RowBase[]{
            row(leftIndexRowType, 2L, 500L, "xxx", 999L, 1009L),
            row(leftIndexRowType, 2L, 500L, "xxx", 999L, 1010L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = intersectPlan(2, IntersectOption.OUTPUT_RIGHT, false, false);
        expected = new RowBase[]{
            row(leftIndexRowType, 2L, 500L, "xxx", 999L, 1009L),
            row(leftIndexRowType, 2L, 500L, "xxx", 999L, 1010L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = intersectPlan(2, IntersectOption.OUTPUT_LEFT, true, true);
        expected = new RowBase[]{
            row(leftIndexRowType, 2L, 500L, "xxx", 999L, 1009L),
            row(leftIndexRowType, 2L, 500L, "xxx", 999L, 1010L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = intersectPlan(2, IntersectOption.OUTPUT_RIGHT, true, true);
        expected = new RowBase[]{
            row(leftIndexRowType, 2L, 500L, "xxx", 999L, 1009L),
            row(leftIndexRowType, 2L, 500L, "xxx", 999L, 1010L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = intersectPlan(2, IntersectOption.OUTPUT_LEFT, false, true);
        expected = new RowBase[]{
            row(leftIndexRowType, 2L, 500L, "xxx", 999L, 1009L),
            row(leftIndexRowType, 2L, 500L, "xxx", 999L, 1010L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = intersectPlan(2, IntersectOption.OUTPUT_RIGHT, false, true);
        expected = new RowBase[]{
            row(leftIndexRowType, 2L, 500L, "xxx", 999L, 1009L),
            row(leftIndexRowType, 2L, 500L, "xxx", 999L, 1010L),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void test3()
    {
        plan = intersectPlan(3, IntersectOption.OUTPUT_LEFT, true, false);
        expected = new RowBase[]{
            row(leftIndexRowType, 3L, 500L, "x", 902L, 1017L),
            row(leftIndexRowType, 3L, 500L, "x", 902L, 1018L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = intersectPlan(3, IntersectOption.OUTPUT_RIGHT, true, false);
        expected = new RowBase[]{
            row(leftIndexRowType, 3L, 500L, "x", 902L, 1017L),
            row(leftIndexRowType, 3L, 500L, "x", 902L, 1018L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = intersectPlan(3, IntersectOption.OUTPUT_LEFT, false, false);
        expected = new RowBase[]{
            row(leftIndexRowType, 3L, 500L, "x", 902L, 1017L),
            row(leftIndexRowType, 3L, 500L, "x", 902L, 1018L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = intersectPlan(3, IntersectOption.OUTPUT_RIGHT, false, false);
        expected = new RowBase[]{
            row(leftIndexRowType, 3L, 500L, "x", 902L, 1017L),
            row(leftIndexRowType, 3L, 500L, "x", 902L, 1018L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = intersectPlan(3, IntersectOption.OUTPUT_LEFT, true, true);
        expected = new RowBase[]{
            row(leftIndexRowType, 3L, 500L, "x", 902L, 1017L),
            row(leftIndexRowType, 3L, 500L, "x", 902L, 1018L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = intersectPlan(3, IntersectOption.OUTPUT_RIGHT, true, true);
        expected = new RowBase[]{
            row(leftIndexRowType, 3L, 500L, "x", 902L, 1017L),
            row(leftIndexRowType, 3L, 500L, "x", 902L, 1018L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = intersectPlan(3, IntersectOption.OUTPUT_LEFT, false, true);
        expected = new RowBase[]{
            row(leftIndexRowType, 3L, 500L, "x", 902L, 1017L),
            row(leftIndexRowType, 3L, 500L, "x", 902L, 1018L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = intersectPlan(3, IntersectOption.OUTPUT_RIGHT, false, true);
        expected = new RowBase[]{
            row(leftIndexRowType, 3L, 500L, "x", 902L, 1017L),
            row(leftIndexRowType, 3L, 500L, "x", 902L, 1018L),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void testCursor()
    {
        plan = intersectPlan(3, IntersectOption.OUTPUT_LEFT, true, false);
        expected = new RowBase[]{
            row(leftIndexRowType, 3L, 500L, "x", 902L, 1017L),
            row(leftIndexRowType, 3L, 500L, "x", 902L, 1018L),
        };
        CursorLifecycleTestCase testCase = new CursorLifecycleTestCase()
        {
            @Override
            public RowBase[] firstExpectedRows()
            {
                return expected;
            }
        };
        testCursorLifecycle(plan, testCase);
        plan = intersectPlan(3, IntersectOption.OUTPUT_LEFT, true, true);
        expected = new RowBase[]{
            row(leftIndexRowType, 3L, 500L, "x", 902L, 1017L),
            row(leftIndexRowType, 3L, 500L, "x", 902L, 1018L),
        };
        testCase = new CursorLifecycleTestCase()
        {
            @Override
            public RowBase[] firstExpectedRows()
            {
                return expected;
            }
        };
        testCursorLifecycle(plan, testCase);
    }

    private Operator intersectPlan(int testId, IntersectOption side, boolean k2Ascending, boolean skipScan)
    {
        Ordering leftOrdering = 
            ordering(field(leftIndexRowType, 0), true,  // test
                     field(leftIndexRowType, 1), true,  // l1
                     field(leftIndexRowType, 2), k2Ascending,  // l2
                     field(leftIndexRowType, 3), true,  // l3
                     field(leftIndexRowType, 4), true); // id
        Ordering rightOrdering = 
            ordering(field(rightIndexRowType, 0), true,  // test
                     field(rightIndexRowType, 1), true,  // r1
                     field(rightIndexRowType, 2), k2Ascending,  // r2
                     field(rightIndexRowType, 3), true,  // r3
                     field(rightIndexRowType, 4), true); // id
        boolean ascending[] = new boolean[]{true, k2Ascending, true};
        Operator plan =
            intersect_Ordered(
                indexScan_Default(
                    leftIndexRowType,
                    eq(leftIndexRowType, testId),
                    leftOrdering),
                indexScan_Default(
                    rightIndexRowType,
                    eq(rightIndexRowType, testId),
                    rightOrdering),
                leftIndexRowType,
                rightIndexRowType,
                4,
                4,
                ascending,
                JoinType.INNER_JOIN,
                EnumSet.of(side, 
                           skipScan ? IntersectOption.SKIP_SCAN : IntersectOption.SEQUENTIAL_SCAN));
        return plan;
    }

    private IndexKeyRange eq(IndexRowType indexRowType, long testId)
    {
        IndexBound testBound = new IndexBound(row(indexRowType, testId), new SetColumnSelector(0));
        return IndexKeyRange.bounded(indexRowType, testBound, true, testBound, true);
    }

    private Ordering ordering(Object... objects)
    {
        Ordering ordering = API.ordering();
        int i = 0;
        while (i < objects.length) {
            Expression expression = (Expression) objects[i++];
            Boolean ascending = (Boolean) objects[i++];
            ordering.append(expression, ascending);
        }
        return ordering;
    }

    private int t;
    private RowType tRowType;
    private IndexRowType leftIndexRowType;
    private IndexRowType rightIndexRowType;
    private Operator plan;
    private RowBase[] expected;
}
