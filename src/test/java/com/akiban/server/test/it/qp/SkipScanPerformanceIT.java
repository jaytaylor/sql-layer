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
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.expression.Expression;
import com.akiban.util.tap.Tap;
import com.akiban.util.tap.TapReport;
import org.junit.Before;
import org.junit.Test;

import java.util.EnumSet;

import static com.akiban.qp.operator.API.*;
import static com.akiban.server.test.ExpressionGenerators.field;
import static java.lang.Math.abs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SkipScanPerformanceIT extends OperatorITBase
{
    @Before
    public void before()
    {
        t = createTable(
            "schema", "t",
            "id int not null",
            "x int",
            "y int",
            "z int",
            "primary key(id)");
        createIndex("schema", "t", "idx_x", "x");
        createIndex("schema", "t", "idx_y", "y");
        createIndex("schema", "t", "idx_z", "z");
        schema = new Schema(ais());
        tRowType = schema.userTableRowType(userTable(t));
        tIdIndexRowType = indexType(t, "id");
        tXIndexRowType = indexType(t, "x");
        tYIndexRowType = indexType(t, "y");
        tZIndexRowType = indexType(t, "z");
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
    }

    private static final IntersectOption LEFT = IntersectOption.OUTPUT_LEFT;
    private static final IntersectOption RIGHT = IntersectOption.OUTPUT_RIGHT;

    private int t;
    private int child;
    private RowType tRowType;
    private RowType childRowType;
    private IndexRowType tIdIndexRowType;
    private IndexRowType tXIndexRowType;
    private IndexRowType tYIndexRowType;
    private IndexRowType tZIndexRowType;

    // Tests are in pairs, with different values of N. This proves that SEQUENTIAL_SCAN costs increase linearly with N,
    // but SKIP_SCAN costs are constant (due to the way the test data is constructed).

    @Test
    public void testIntersect100()
    {
        final int N = 100;
        final int X = 888;
        final int Y = 999;
        // Create a set of N rows. Each set is structured as follows:
        //     id[0]      888    999      null
        //     id[1]      888    null     null
        //     ...
        //     id[N-1]    888    999      null
        assertTrue(N > 1);
        db = new NewRow[N];
        for (int id = 0; id < N; id++) {
            Integer y = id == 0 || id == N - 1 ? Y : null;
            db[id] = createNewRow(t, id, X, y, null);
        }
        use(db);
        // N rows from left index scan, 2 from right, 1 to realize scan is done.
        testIntersect(X, Y, false, N + 3);
        // 6 with skip scan: 1st, 2nd and last record on left, 1st and last record on right, 1 to realize scan is done.
        testIntersect(X, Y, true, 6);
    }

    @Test
    public void testIntersect1000()
    {
        final int N = 1000;
        final int X = 888;
        final int Y = 999;
        // Create a set of N rows. Each set is structured as follows:
        //     id[0]      888    999      null
        //     id[1]      888    null     null
        //     ...
        //     id[N-1]    888    999      null
        assertTrue(N > 1);
        db = new NewRow[N];
        for (int id = 0; id < N; id++) {
            Integer y = id == 0 || id == N - 1 ? Y : null;
            db[id] = createNewRow(t, id, X, y, null);
        }
        use(db);
        // N rows from left index scan, 2 from right, 1 to realize scan is done.
        testIntersect(X, Y, false, N + 3);
        // 6 with skip scan: 1st, 2nd and last record on left, 1st and last record on right, 1 to realize scan is done.
        testIntersect(X, Y, true, 6);
    }

    @Test
    public void testTwoIntersects60()
    {
        final int N = 60;
        final int X = 777;
        final int Y = 888;
        final int Z = 999;
        // Create a set of N rows, N divisible by 3.
        // - Scan of x retrieves rows [0, 2N/3)
        // - Scan of y retrieves rows [N/3, N)
        // - Scan of z retrieves rows N/3-1, N/3, N/3+1
        assertTrue((N % 3) == 0);
        assertTrue(N / 3 >= 2);
        db = new NewRow[N];
        for (int id = 0; id < N; id++) {
            Integer x = id < 2 * N / 3 ? X : null;
            Integer y = id >= N / 3 ? Y : null;
            Integer z = abs(id - N/3) <= 1 ? Z : null;
            NewRow row = createNewRow(t, id, x, y, z);
            db[id] = row;
        }
        use(db);
        // x scan: N/3 + 3
        // y scan: 3
        // z scan: 3
        // Realize two intersects are done: 2
        testTwoIntersects(X, Y, Z, false, N / 3 + 11);
        // x scan: 1st row, 3 rows starting at N/3
        // y scan: 3 rows starting at N/3
        // z scan: 3 rows
        // Realize two intersects are done: 2
        testTwoIntersects(X, Y, Z, true, 12);
    }

    @Test
    public void testTwoIntersects600()
    {
        final int N = 600;
        final int X = 777;
        final int Y = 888;
        final int Z = 999;
        // Create a set of N rows, N divisible by 3.
        // - Scan of x retrieves rows [0, 2N/3)
        // - Scan of y retrieves rows [N/3, N)
        // - Scan of z retrieves rows N/3-1, N/3, N/3+1
        assertTrue((N % 3) == 0);
        assertTrue(N / 3 >= 2);
        db = new NewRow[N];
        for (int id = 0; id < N; id++) {
            Integer x = id < 2 * N / 3 ? X : null;
            Integer y = id >= N / 3 ? Y : null;
            Integer z = abs(id - N/3) <= 1 ? Z : null;
            NewRow row = createNewRow(t, id, x, y, z);
            db[id] = row;
        }
        use(db);
        // x scan: N/3 + 3
        // y scan: 3
        // z scan: 3
        // Realize two intersects are done: 2
        testTwoIntersects(X, Y, Z, false, N / 3 + 11);
        // x scan: 1st row, 3 rows starting at N/3
        // y scan: 3 rows starting at N/3
        // z scan: 3 rows
        // Realize two intersects are done: 2
        testTwoIntersects(X, Y, Z, true, 12);
    }

    private void testIntersect(int x, int y, boolean skipScan, int expectedIndexRows)
    {
        Operator plan = intersectXY(x, y, skipScan);
        Tap.reset("operator.*");
        Tap.setEnabled("operator.*", true);
        Cursor cursor = cursor(plan, queryContext);
        cursor.open();
        while (cursor.next() != null);
        TapReport[] reports = Tap.getReport("operator.*");
        for (TapReport report : reports) {
            if (report.getName().equals("operator: IndexScan_Default next")) {
                assertEquals(expectedIndexRows, report.getInCount());
            }
        }
    }

    private void testTwoIntersects(int x, int y, int z, boolean skipScan, int expectedIndexRows)
    {
        Operator plan = intersectXYintersectZ(x, y, z, skipScan);
        Tap.reset("operator.*");
        Tap.setEnabled("operator.*", true);
        Cursor cursor = cursor(plan, queryContext);
        cursor.open();
        Row row;
        while ((row = cursor.next()) != null);
        TapReport[] reports = Tap.getReport("operator.*");
        for (TapReport report : reports) {
            if (report.getName().equals("operator: IndexScan_Default next")) {
                assertEquals(expectedIndexRows, report.getInCount());
            }
        }
    }

    private Operator intersectXY(int x, int y, boolean skipScan)
    {
        Ordering xOrdering = new Ordering();
        xOrdering.append(field(tXIndexRowType, 1), true);
        Ordering yOrdering = new Ordering();
        yOrdering.append(field(tYIndexRowType, 1), true);
        return
            intersect_Ordered(
                indexScan_Default(
                    tXIndexRowType,
                    xEq(x),
                    xOrdering),
                indexScan_Default(
                    tYIndexRowType,
                    yEq(y),
                    yOrdering),
                tXIndexRowType,
                tYIndexRowType,
                1,
                1,
                ascending(true),
                JoinType.INNER_JOIN,
                EnumSet.of(
                    skipScan ? IntersectOption.SKIP_SCAN : IntersectOption.SEQUENTIAL_SCAN,
                    IntersectOption.OUTPUT_LEFT));
    }

    private Operator intersectXYintersectZ(int x, int y, int z, boolean skip)
    {
        Ordering xOrdering = new Ordering();
        xOrdering.append(field(tXIndexRowType, 1), true);
        Ordering yOrdering = new Ordering();
        yOrdering.append(field(tYIndexRowType, 1), true);
        Ordering zOrdering = new Ordering();
        zOrdering.append(field(tZIndexRowType, 1), true);
        IntersectOption scanType = skip ? IntersectOption.SKIP_SCAN : IntersectOption.SEQUENTIAL_SCAN;
        return intersect_Ordered(
            intersect_Ordered(
                indexScan_Default(
                    tXIndexRowType,
                    xEq(x),
                    xOrdering),
                indexScan_Default(
                    tYIndexRowType,
                    yEq(y),
                    yOrdering),
                tXIndexRowType,
                tYIndexRowType,
                1,
                1,
                ascending(true),
                JoinType.INNER_JOIN,
                EnumSet.of(scanType, LEFT)),
            indexScan_Default(
                tZIndexRowType,
                zEq(z),
                zOrdering),
            tXIndexRowType,
            tZIndexRowType,
            1,
            1,
            ascending(true),
            JoinType.INNER_JOIN,
            EnumSet.of(scanType, LEFT));
    }

    private Operator unionXXunionX(int x1, int x2, int x3)
    {
        Ordering ordering = new Ordering();
        ordering.append(field(tXIndexRowType, 1), true);
        return union_Ordered(
            union_Ordered(
                indexScan_Default(
                    tXIndexRowType,
                    xEq(x1),
                    ordering),
                indexScan_Default(
                    tXIndexRowType,
                    yEq(x2),
                    ordering),
                tXIndexRowType,
                tXIndexRowType,
                1,
                1,
                ascending(true)),
            indexScan_Default(
                tXIndexRowType,
                xEq(x3),
                ordering),
            tXIndexRowType,
            tXIndexRowType,
            1,
            1,
            ascending(true));
    }

    private Operator intersectXYunionX(int x1, int y, int x2, boolean skip)
    {
        Ordering xOrdering = new Ordering();
        xOrdering.append(field(tXIndexRowType, 1), true);
        Ordering yOrdering = new Ordering();
        yOrdering.append(field(tYIndexRowType, 1), true);
        IntersectOption scanType = skip ? IntersectOption.SKIP_SCAN : IntersectOption.SEQUENTIAL_SCAN;
        return union_Ordered(
            intersect_Ordered(
                indexScan_Default(
                    tXIndexRowType,
                    xEq(x1),
                    xOrdering),
                indexScan_Default(
                    tYIndexRowType,
                    yEq(y),
                    yOrdering),
                tXIndexRowType,
                tYIndexRowType,
                1,
                1,
                ascending(true),
                JoinType.INNER_JOIN,
                EnumSet.of(scanType, LEFT)),
            indexScan_Default(
                tXIndexRowType,
                xEq(x2),
                xOrdering),
            tXIndexRowType,
            tXIndexRowType,
            1,
            1,
            ascending(true));
    }

    private Operator unionXXintersectY(int x1, int x2, int y, IntersectOption intersectOutput, boolean skip)
    {
        Ordering xOrdering = new Ordering();
        xOrdering.append(field(tXIndexRowType, 1), true);
        Ordering yOrdering = new Ordering();
        yOrdering.append(field(tYIndexRowType, 1), true);
        IntersectOption scanType = skip ? IntersectOption.SKIP_SCAN : IntersectOption.SEQUENTIAL_SCAN;
        return intersect_Ordered(
            union_Ordered(
                indexScan_Default(
                    tXIndexRowType,
                    xEq(x1),
                    xOrdering),
                indexScan_Default(
                    tXIndexRowType,
                    xEq(x2),
                    xOrdering),
                tXIndexRowType,
                tXIndexRowType,
                1,
                1,
                ascending(true)),
            indexScan_Default(
                tYIndexRowType,
                yEq(y),
                yOrdering),
            tXIndexRowType,
            tYIndexRowType,
            1,
            1,
            ascending(true),
            JoinType.INNER_JOIN,
            EnumSet.of(scanType, intersectOutput));
    }

    private IndexKeyRange xEq(long x)
    {
        IndexBound bound = new IndexBound(row(tXIndexRowType, x), new SetColumnSelector(0));
        return IndexKeyRange.bounded(tXIndexRowType, bound, true, bound, true);
    }

    private IndexKeyRange yEq(long y)
    {
        IndexBound bound = new IndexBound(row(tYIndexRowType, y), new SetColumnSelector(0));
        return IndexKeyRange.bounded(tYIndexRowType, bound, true, bound, true);
    }

    private IndexKeyRange zEq(long z)
    {
        IndexBound bound = new IndexBound(row(tZIndexRowType, z), new SetColumnSelector(0));
        return IndexKeyRange.bounded(tZIndexRowType, bound, true, bound, true);
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

    private boolean[] ascending(boolean... ascending)
    {
        return ascending;
    }
}
