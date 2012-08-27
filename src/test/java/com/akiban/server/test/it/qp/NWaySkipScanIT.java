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

import com.akiban.ais.model.GroupTable;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class NWaySkipScanIT extends OperatorITBase
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
        schema = new Schema(rowDefCache().ais());
        tRowType = schema.userTableRowType(userTable(t));
        tIdIndexRowType = indexType(t, "id");
        tXIndexRowType = indexType(t, "x");
        tYIndexRowType = indexType(t, "y");
        tZIndexRowType = indexType(t, "z");
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
        db = new NewRow[] {
            createNewRow(t, 1000L, 71L, 81L, 91L),
            createNewRow(t, 1001L, 71L, 81L, 92L),
            createNewRow(t, 1002L, 71L, 82L, 91L),
            createNewRow(t, 1003L, 71L, 82L, 92L),
            createNewRow(t, 1004L, 72L, 81L, 91L),
            createNewRow(t, 1005L, 72L, 81L, 92L),
            createNewRow(t, 1006L, 72L, 82L, 91L),
            createNewRow(t, 1007L, 72L, 82L, 92L),
            createNewRow(t, 1008L, 73L, null, null),
        };
        use(db);
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

    @Test
    public void testTwoIntersects()
    {
        for (int x = 71; x <= 72; x++) {
            for (int y = 81; y <= 82; y++) {
                for (int z = 91; z <= 92; z++) {
                    testTwoIntersects(x, intersectXYintersectZ(x, y, LEFT, z, LEFT, true));
                    testTwoIntersects(x, intersectXYintersectZ(x, y, LEFT, z, LEFT, false));
                    testTwoIntersects(z, intersectXYintersectZ(x, y, LEFT, z, RIGHT, true));
                    testTwoIntersects(z, intersectXYintersectZ(x, y, LEFT, z, RIGHT, false));
                    testTwoIntersects(y, intersectXYintersectZ(x, y, RIGHT, z, LEFT, true));
                    testTwoIntersects(y, intersectXYintersectZ(x, y, RIGHT, z, LEFT, false));
                    testTwoIntersects(z, intersectXYintersectZ(x, y, RIGHT, z, RIGHT, true));
                    testTwoIntersects(z, intersectXYintersectZ(x, y, RIGHT, z, RIGHT, false));
                }
            }
        }
    }

    @Test
    public void testTwoUnions()
    {
        Operator plan = unionXXunionX(71, 72, 73);
        RowBase[] expected = new RowBase[] {
            row(tXIndexRowType, 71L, 1000L),
            row(tXIndexRowType, 71L, 1001L),
            row(tXIndexRowType, 71L, 1002L),
            row(tXIndexRowType, 71L, 1003L),
            row(tXIndexRowType, 72L, 1004L),
            row(tXIndexRowType, 72L, 1005L),
            row(tXIndexRowType, 72L, 1006L),
            row(tXIndexRowType, 72L, 1007L),
            row(tXIndexRowType, 73L, 1008L),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void testIntersectThenUnion()
    {
        RowBase[] expected = new RowBase[] {
            row(tXIndexRowType, 71L, 1000L),
            row(tXIndexRowType, 71L, 1001L),
            row(tXIndexRowType, 72L, 1004L),
            row(tXIndexRowType, 72L, 1005L),
            row(tXIndexRowType, 72L, 1006L),
            row(tXIndexRowType, 72L, 1007L),
        };
        compareRows(expected, cursor(intersectXYunionX(71, 81, 72, false), queryContext));
        compareRows(expected, cursor(intersectXYunionX(71, 81, 72, true), queryContext));
    }

    @Test
    public void testIntersectWithEmptyInputThenUnion()
    {
        RowBase[] expected = new RowBase[] {
            row(tXIndexRowType, 72L, 1004L),
            row(tXIndexRowType, 72L, 1005L),
            row(tXIndexRowType, 72L, 1006L),
            row(tXIndexRowType, 72L, 1007L),
        };
        // Left input to intersection is empty
        {
            compareRows(expected, cursor(intersectXYunionX(99, 81, 72, false), queryContext));
            compareRows(expected, cursor(intersectXYunionX(99, 81, 72, true), queryContext));
        }
        // Right input to intersection is empty
        {
            compareRows(expected, cursor(intersectXYunionX(71, 99, 72, false), queryContext));
            compareRows(expected, cursor(intersectXYunionX(71, 99, 72, true), queryContext));
        }
        // Both inputs to intersection are empty
        {
            compareRows(expected, cursor(intersectXYunionX(99, 99, 72, false), queryContext));
            compareRows(expected, cursor(intersectXYunionX(99, 99, 72, true), queryContext));
        }
    }

    @Test
    public void testUnionThenIntersect()
    {
        {
            RowBase[] expected = {
                row(tXIndexRowType, 71, 1000L),
                row(tXIndexRowType, 71, 1001L),
                row(tXIndexRowType, 72, 1004L),
                row(tXIndexRowType, 72, 1005L),
            };
            compareRows(expected, cursor(unionXXintersectY(71, 72, 81, LEFT, false), queryContext));
            compareRows(expected, cursor(unionXXintersectY(71, 72, 81, LEFT, true), queryContext));
        }
        {
            RowBase[] expected = {
                row(tXIndexRowType, 81, 1000L),
                row(tXIndexRowType, 81, 1001L),
                row(tXIndexRowType, 81, 1004L),
                row(tXIndexRowType, 81, 1005L),
            };
            compareRows(expected, cursor(unionXXintersectY(71, 72, 81, RIGHT, false), queryContext));
            compareRows(expected, cursor(unionXXintersectY(71, 72, 81, RIGHT, true), queryContext));
        }
        {
            RowBase[] expected = {
                row(tXIndexRowType, 71, 1002L),
                row(tXIndexRowType, 71, 1003L),
                row(tXIndexRowType, 72, 1006L),
                row(tXIndexRowType, 72, 1007L),
            };
            compareRows(expected, cursor(unionXXintersectY(71, 72, 82, LEFT, false), queryContext));
            compareRows(expected, cursor(unionXXintersectY(71, 72, 82, LEFT, true), queryContext));
        }
        {
            RowBase[] expected = {
                row(tXIndexRowType, 82, 1002L),
                row(tXIndexRowType, 82, 1003L),
                row(tXIndexRowType, 82, 1006L),
                row(tXIndexRowType, 82, 1007L),
            };
            compareRows(expected, cursor(unionXXintersectY(71, 72, 82, RIGHT, false), queryContext));
            compareRows(expected, cursor(unionXXintersectY(71, 72, 82, RIGHT, true), queryContext));
        }
    }

    @Test
    public void testUnionWithEmptyInputThenIntersect()
    {
        // Left input to union is empty
        {
            RowBase[] expected = {
                row(tXIndexRowType, 72, 1004L),
                row(tXIndexRowType, 72, 1005L),
            };
            compareRows(expected, cursor(unionXXintersectY(99, 72, 81, LEFT, false), queryContext));
            compareRows(expected, cursor(unionXXintersectY(99, 72, 81, LEFT, true), queryContext));
        }
        // Right input to union is empty
        {
            RowBase[] expected = {
                row(tXIndexRowType, 71, 1000L),
                row(tXIndexRowType, 71, 1001L),
            };
            compareRows(expected, cursor(unionXXintersectY(71, 99, 81, LEFT, false), queryContext));
            compareRows(expected, cursor(unionXXintersectY(71, 99, 81, LEFT, true), queryContext));
        }
        // Both inputs to union are empty
        {
            RowBase[] expected = {
            };
            compareRows(expected, cursor(unionXXintersectY(99, 99, 81, LEFT, false), queryContext));
            compareRows(expected, cursor(unionXXintersectY(99, 99, 81, LEFT, true), queryContext));
        }
    }

    private void testTwoIntersects(long key, Operator plan)
    {
        Cursor cursor = cursor(plan, queryContext);
        cursor.open();
        RowBase row = cursor.next();
        assertEquals(key, row.eval(0).getInt());
        assertNull(cursor.next());
    }

    private Operator intersectXYintersectZ(int x, int y, IntersectOption xyOutput, int z, IntersectOption xyzOutput, boolean skip)
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
                EnumSet.of(scanType, xyOutput)),
            indexScan_Default(
                tZIndexRowType,
                zEq(z),
                zOrdering),
            xyOutput == LEFT ? tXIndexRowType : tYIndexRowType,
            tZIndexRowType,
            1,
            1,
            ascending(true),
            JoinType.INNER_JOIN,
            EnumSet.of(scanType, xyzOutput));
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
