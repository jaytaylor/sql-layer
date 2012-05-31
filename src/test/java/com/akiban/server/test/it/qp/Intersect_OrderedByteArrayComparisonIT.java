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
import org.junit.Test;

import java.util.EnumSet;

import static com.akiban.qp.operator.API.*;
import static com.akiban.server.expression.std.Expressions.field;

// Testing Intersect_Ordered key comparisons, which are done at the Persistit level.

public class Intersect_OrderedByteArrayComparisonIT extends OperatorITBase
{
    @Before
    public void before()
    {
        t = createTable(
            "schema", "t",
            "id int not null",
            "test int", // test case
            "input_side int", // separate left (0) and right (1) inputs
            "k1 int",
            "k2 varchar(10)",
            "k3 int",
            "primary key(id)");
        createIndex("schema", "t", "idx", "test", "input_side", "k1", "k2", "k3");
        schema = new Schema(rowDefCache().ais());
        tRowType = schema.userTableRowType(userTable(t));
        idxRowType = indexType(t, "test", "input_side", "k1", "k2", "k3");
        coi = groupTable(t);
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
        db = new NewRow[]{
            // 1: Comparisons need only examine k1 on a mismatch.
            createNewRow(t, 1000, 1, 0, 500, "x", 999),
            createNewRow(t, 1001, 1, 0, 502, "x", 999),
            createNewRow(t, 1002, 1, 0, 502, "x", 999),
            createNewRow(t, 1003, 1, 0, 504, "x", 999),
            createNewRow(t, 1004, 1, 1, 501, "x", 999),
            createNewRow(t, 1005, 1, 1, 502, "x", 999),
            createNewRow(t, 1006, 1, 1, 502, "x", 999),
            createNewRow(t, 1007, 1, 1, 503, "x", 999),
            // 2: k1 always equal, k2s differ in length
            createNewRow(t, 1008, 2, 0, 500, "x", 999),
            createNewRow(t, 1009, 2, 0, 500, "xxx", 999),
            createNewRow(t, 1010, 2, 0, 500, "xxx", 999),
            createNewRow(t, 1011, 2, 0, 500, "xxxxx", 999),
            createNewRow(t, 1012, 2, 1, 500, "xx", 999),
            createNewRow(t, 1013, 2, 1, 500, "xxx", 999),
            createNewRow(t, 1014, 2, 1, 500, "xxx", 999),
            createNewRow(t, 1015, 2, 1, 500, "xxxx", 999),
            // 3: k1, k2 always match, difference is in k3
            createNewRow(t, 1016, 3, 0, 500, "x", 900),
            createNewRow(t, 1017, 3, 0, 500, "x", 902),
            createNewRow(t, 1018, 3, 0, 500, "x", 902),
            createNewRow(t, 1019, 3, 0, 500, "x", 904),
            createNewRow(t, 1020, 3, 1, 500, "x", 901),
            createNewRow(t, 1021, 3, 1, 500, "x", 902),
            createNewRow(t, 1022, 3, 1, 500, "x", 902),
            createNewRow(t, 1023, 3, 1, 500, "x", 903),
        };
        use(db);
    }

    @Test
    public void test1()
    {
        plan = intersectPlan(1, IntersectOption.OUTPUT_LEFT, true, false);
        expected = new RowBase[]{
            row(idxRowType, 1L, 0L, 502L, "x", 999L, 1001L),
            row(idxRowType, 1L, 0L, 502L, "x", 999L, 1002L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = intersectPlan(1, IntersectOption.OUTPUT_RIGHT, true, false);
        expected = new RowBase[]{
            row(idxRowType, 1L, 1L, 502L, "x", 999L, 1005L),
            row(idxRowType, 1L, 1L, 502L, "x", 999L, 1006L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = intersectPlan(1, IntersectOption.OUTPUT_LEFT, false, false);
        expected = new RowBase[]{
            row(idxRowType, 1L, 0L, 502L, "x", 999L, 1001L),
            row(idxRowType, 1L, 0L, 502L, "x", 999L, 1002L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = intersectPlan(1, IntersectOption.OUTPUT_RIGHT, false, false);
        expected = new RowBase[]{
            row(idxRowType, 1L, 1L, 502L, "x", 999L, 1005L),
            row(idxRowType, 1L, 1L, 502L, "x", 999L, 1006L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = intersectPlan(1, IntersectOption.OUTPUT_LEFT, true, true);
        expected = new RowBase[]{
            row(idxRowType, 1L, 0L, 502L, "x", 999L, 1001L),
            row(idxRowType, 1L, 0L, 502L, "x", 999L, 1002L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = intersectPlan(1, IntersectOption.OUTPUT_RIGHT, true, true);
        expected = new RowBase[]{
            row(idxRowType, 1L, 1L, 502L, "x", 999L, 1005L),
            row(idxRowType, 1L, 1L, 502L, "x", 999L, 1006L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = intersectPlan(1, IntersectOption.OUTPUT_LEFT, false, true);
        expected = new RowBase[]{
            row(idxRowType, 1L, 0L, 502L, "x", 999L, 1001L),
            row(idxRowType, 1L, 0L, 502L, "x", 999L, 1002L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = intersectPlan(1, IntersectOption.OUTPUT_RIGHT, false, true);
        expected = new RowBase[]{
            row(idxRowType, 1L, 1L, 502L, "x", 999L, 1005L),
            row(idxRowType, 1L, 1L, 502L, "x", 999L, 1006L),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void test2()
    {
        plan = intersectPlan(2, IntersectOption.OUTPUT_LEFT, true, false);
        expected = new RowBase[]{
            row(idxRowType, 2L, 0L, 500L, "xxx", 999L, 1009L),
            row(idxRowType, 2L, 0L, 500L, "xxx", 999L, 1010L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = intersectPlan(2, IntersectOption.OUTPUT_RIGHT, true, false);
        expected = new RowBase[]{
            row(idxRowType, 2L, 1L, 500L, "xxx", 999L, 1013L),
            row(idxRowType, 2L, 1L, 500L, "xxx", 999L, 1014L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = intersectPlan(2, IntersectOption.OUTPUT_LEFT, false, false);
        expected = new RowBase[]{
            row(idxRowType, 2L, 0L, 500L, "xxx", 999L, 1009L),
            row(idxRowType, 2L, 0L, 500L, "xxx", 999L, 1010L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = intersectPlan(2, IntersectOption.OUTPUT_RIGHT, false, false);
        expected = new RowBase[]{
            row(idxRowType, 2L, 1L, 500L, "xxx", 999L, 1013L),
            row(idxRowType, 2L, 1L, 500L, "xxx", 999L, 1014L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = intersectPlan(2, IntersectOption.OUTPUT_LEFT, true, true);
        expected = new RowBase[]{
            row(idxRowType, 2L, 0L, 500L, "xxx", 999L, 1009L),
            row(idxRowType, 2L, 0L, 500L, "xxx", 999L, 1010L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = intersectPlan(2, IntersectOption.OUTPUT_RIGHT, true, true);
        expected = new RowBase[]{
            row(idxRowType, 2L, 1L, 500L, "xxx", 999L, 1013L),
            row(idxRowType, 2L, 1L, 500L, "xxx", 999L, 1014L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = intersectPlan(2, IntersectOption.OUTPUT_LEFT, false, true);
        expected = new RowBase[]{
            row(idxRowType, 2L, 0L, 500L, "xxx", 999L, 1009L),
            row(idxRowType, 2L, 0L, 500L, "xxx", 999L, 1010L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = intersectPlan(2, IntersectOption.OUTPUT_RIGHT, false, true);
        expected = new RowBase[]{
            row(idxRowType, 2L, 1L, 500L, "xxx", 999L, 1013L),
            row(idxRowType, 2L, 1L, 500L, "xxx", 999L, 1014L),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void test3()
    {
        plan = intersectPlan(3, IntersectOption.OUTPUT_LEFT, true, false);
        expected = new RowBase[]{
            row(idxRowType, 3L, 0L, 500L, "x", 902L, 1017L),
            row(idxRowType, 3L, 0L, 500L, "x", 902L, 1018L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = intersectPlan(3, IntersectOption.OUTPUT_RIGHT, true, false);
        expected = new RowBase[]{
            row(idxRowType, 3L, 1L, 500L, "x", 902L, 1021L),
            row(idxRowType, 3L, 1L, 500L, "x", 902L, 1022L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = intersectPlan(3, IntersectOption.OUTPUT_LEFT, false, false);
        expected = new RowBase[]{
            row(idxRowType, 3L, 0L, 500L, "x", 902L, 1017L),
            row(idxRowType, 3L, 0L, 500L, "x", 902L, 1018L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = intersectPlan(3, IntersectOption.OUTPUT_RIGHT, false, false);
        expected = new RowBase[]{
            row(idxRowType, 3L, 1L, 500L, "x", 902L, 1021L),
            row(idxRowType, 3L, 1L, 500L, "x", 902L, 1022L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = intersectPlan(3, IntersectOption.OUTPUT_LEFT, true, true);
        expected = new RowBase[]{
            row(idxRowType, 3L, 0L, 500L, "x", 902L, 1017L),
            row(idxRowType, 3L, 0L, 500L, "x", 902L, 1018L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = intersectPlan(3, IntersectOption.OUTPUT_RIGHT, true, true);
        expected = new RowBase[]{
            row(idxRowType, 3L, 1L, 500L, "x", 902L, 1021L),
            row(idxRowType, 3L, 1L, 500L, "x", 902L, 1022L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = intersectPlan(3, IntersectOption.OUTPUT_LEFT, false, true);
        expected = new RowBase[]{
            row(idxRowType, 3L, 0L, 500L, "x", 902L, 1017L),
            row(idxRowType, 3L, 0L, 500L, "x", 902L, 1018L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = intersectPlan(3, IntersectOption.OUTPUT_RIGHT, false, true);
        expected = new RowBase[]{
            row(idxRowType, 3L, 1L, 500L, "x", 902L, 1021L),
            row(idxRowType, 3L, 1L, 500L, "x", 902L, 1022L),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void testCursor()
    {
        plan = intersectPlan(3, IntersectOption.OUTPUT_LEFT, true, false);
        expected = new RowBase[]{
            row(idxRowType, 3L, 0L, 500L, "x", 902L, 1017L),
            row(idxRowType, 3L, 0L, 500L, "x", 902L, 1018L),
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
            row(idxRowType, 3L, 0L, 500L, "x", 902L, 1017L),
            row(idxRowType, 3L, 0L, 500L, "x", 902L, 1018L),
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
        Ordering ordering = ordering(field(idxRowType, 0), true,  // test
                                     field(idxRowType, 1), true,  // input_side
                                     field(idxRowType, 2), true,  // k1
                                     field(idxRowType, 3), k2Ascending,  // k2
                                     field(idxRowType, 4), true,  // k3
                                     field(idxRowType, 5), true); // id
        boolean ascending[] = new boolean[]{true, k2Ascending, true};
        Operator plan =
            intersect_Ordered(
                indexScan_Default(
                    idxRowType,
                    eq(testId, 0),
                    ordering),
                indexScan_Default(
                    idxRowType,
                    eq(testId, 1),
                    ordering),
                idxRowType,
                idxRowType,
                4,
                4,
                ascending,
                JoinType.INNER_JOIN,
                EnumSet.of(side, 
                           skipScan ? IntersectOption.SKIP_SCAN : IntersectOption.SEQUENTIAL_SCAN));
        return plan;
    }

    private IndexKeyRange eq(long testId, long side)
    {
        IndexBound xBound = new IndexBound(row(idxRowType, testId, side), new SetColumnSelector(0, 1));
        return IndexKeyRange.bounded(idxRowType, xBound, true, xBound, true);
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
    private IndexRowType idxRowType;
    private Operator plan;
    private RowBase[] expected;
}
