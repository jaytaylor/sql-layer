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
import static com.akiban.server.test.ExpressionGenerators.field;

public class Intersect_OrderedSkipScanIT extends OperatorITBase
{
    @Before
    public void before()
    {
        parent = createTable(
            "schema", "parent",
            "pid int not null primary key",
            "x1 int",
            "x2 int",
            "y int");
        createIndex("schema", "parent", "idx_x", "x1", "x2");
        createIndex("schema", "parent", "idx_y", "y");
        child = createTable(
            "schema", "child",
            "cid int not null primary key",
            "pid int",
            "z int",
            "grouping foreign key (pid) references parent(pid)");
        createIndex("schema", "child", "z", "z");
        schema = new Schema(rowDefCache().ais());
        parentRowType = schema.userTableRowType(userTable(parent));
        childRowType = schema.userTableRowType(userTable(child));
        parentPidIndexRowType = indexType(parent, "pid");
        parentXIndexRowType = indexType(parent, "x1", "x2");
        parentYIndexRowType = indexType(parent, "y");
        childZIndexRowType = indexType(child, "z");
        coi = group(parent);
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
        db = new NewRow[]{
            // 0x: Both index scans empty
            // 1x: Left empty
            createNewRow(parent, 1000L,     -1L, -1L,     11L),
            createNewRow(parent, 1001L,     -1L, -1L,     11L),
            createNewRow(parent, 1002L,     -1L, -1L,     11L),
            // 2x: Right empty
            createNewRow(parent, 2000L,     22L, 22L,     -1L),
            createNewRow(parent, 2001L,     22L, 22L,     -1L),
            createNewRow(parent, 2002L,     22L, 22L,     -1L),
            // 3x: Both non-empty, and no overlap
            createNewRow(parent, 3000L,     31L, 31L,     -1L),
            createNewRow(parent, 3001L,     31L, 31L,     -1L),
            createNewRow(parent, 3002L,     31L, 31L,     -1L),
            createNewRow(parent, 3003L,     9999L, 9999L,     32L),
            createNewRow(parent, 3004L,     9999L, 9999L,     32L),
            createNewRow(parent, 3005L,     9999L, 9999L,     32L),
            // 4x: left contains right
            createNewRow(parent, 4000L,     44L, 44L,     -1L),
            createNewRow(parent, 4001L,     44L, 44L,     44L),
            createNewRow(parent, 4002L,     44L, 44L,     44L),
            createNewRow(parent, 4003L,     44L, 44L,     9999L),
            // 5x: right contains left
            createNewRow(parent, 5000L,     -1L, -1L,     55L),
            createNewRow(parent, 5001L,     55L, 55L,     55L),
            createNewRow(parent, 5002L,     55L, 55L,     55L),
            createNewRow(parent, 5003L,     9999L, 9999L,     55L),
            // 6x: overlap but neither side contains the other
            createNewRow(parent, 6000L,     -1L, -1L,     66L),
            createNewRow(parent, 6001L,     -1L, -1L,     66L),
            createNewRow(parent, 6002L,     66L, 66L,     66L),
            createNewRow(parent, 6003L,     66L, 66L,     66L),
            createNewRow(parent, 6004L,     66L, 66L,     9999L),
            createNewRow(parent, 6005L,     66L, 66L,     9999L),
            // 7x: parent with no children
            createNewRow(parent, 7000L,     70L, 70L,     70L),
            // 8x: parent with children
            createNewRow(parent, 8000L,     88L, 88L,     88L),
            createNewRow(child, 800000L, 8000L, 88L),
            createNewRow(parent, 8001L,     88L, 88L,     88L),
            createNewRow(child, 800100L, 8001L, 88L),
            createNewRow(child, 800101L, 8001L, 88L),
            createNewRow(parent, 8002L,     88L, 88L,     88L),
            createNewRow(child, 800200L, 8002L, 88L),
            createNewRow(child, 800201L, 8002L, 88L),
            createNewRow(child, 800202L, 8002L, 88L),
            // 9x child with no parent
            createNewRow(child, 900000L, 9000L, 99L),
            // 12x right join (child on right)
            createNewRow(child, 1200000L, null, 12L),
            // 13x skip in both directions
            createNewRow(parent, 13000L,     -1L, -1L,     13L),
            createNewRow(parent, 13001L,     -1L, -1L,     13L),
            createNewRow(parent, 13002L,     13L, 13L,     13L),
            createNewRow(parent, 13003L,     13L, 13L,     -1L),
            createNewRow(parent, 13004L,     13L, 13L,     -1L),
            createNewRow(parent, 13005L,     13L, 13L,     13L),
            createNewRow(parent, 13006L,     -1L, -1L,     13L),
            createNewRow(parent, 13007L,     -1L, -1L,     13L),
            createNewRow(parent, 13008L,     -1L, -1L,     13L),
            createNewRow(parent, 13009L,     -1L, -1L,     13L),
            createNewRow(parent, 13010L,     13L, 13L,     13L),
            createNewRow(parent, 13011L,     13L, 13L,     13L),
            createNewRow(parent, 13012L,     13L, 13L,     13L),
            createNewRow(parent, 13013L,     13L, 13L,     -1L),
            createNewRow(parent, 13014L,     13L, 13L,     -1L),
            createNewRow(parent, 13015L,     13L, 13L,     -1L),
        };
        use(db);
    }

    private int parent;
    private int child;
    private RowType parentRowType;
    private RowType childRowType;
    private IndexRowType parentPidIndexRowType;
    private IndexRowType parentXIndexRowType;
    private IndexRowType parentYIndexRowType;
    private IndexRowType childZIndexRowType;

    @Test
    public void test0x()
    {
        RowBase[] expectedX = new RowBase[]{
        };
        compareRows(expectedX, cursor(intersectPxPy(0, true, true, false), queryContext));
        compareRows(expectedX, cursor(intersectPxPy(0, true, true, true), queryContext));
        reverse(expectedX);
        compareRows(expectedX, cursor(intersectPxPy(0, true, false, false), queryContext));
        compareRows(expectedX, cursor(intersectPxPy(0, true, false, true), queryContext));
        RowBase[] expectedY = new RowBase[]{
        };
        compareRows(expectedY, cursor(intersectPxPy(0, false, true, false), queryContext));
        compareRows(expectedY, cursor(intersectPxPy(0, false, true, true), queryContext));
        reverse(expectedY);
        compareRows(expectedY, cursor(intersectPxPy(0, false, false, false), queryContext));
        compareRows(expectedY, cursor(intersectPxPy(0, false, false, true), queryContext));
    }

    @Test
    public void test1x()
    {
        RowBase[] expectedX = new RowBase[]{
        };
        compareRows(expectedX, cursor(intersectPxPy(11, true, true, false), queryContext));
        compareRows(expectedX, cursor(intersectPxPy(11, true, true, true), queryContext));
        reverse(expectedX);
        compareRows(expectedX, cursor(intersectPxPy(11, true, false, false), queryContext));
        compareRows(expectedX, cursor(intersectPxPy(11, true, false, true), queryContext));
        RowBase[] expectedY = new RowBase[]{
        };
        compareRows(expectedY, cursor(intersectPxPy(11, false, true, false), queryContext));
        compareRows(expectedY, cursor(intersectPxPy(11, false, true, true), queryContext));
        reverse(expectedY);
        compareRows(expectedY, cursor(intersectPxPy(11, false, false, false), queryContext));
        compareRows(expectedY, cursor(intersectPxPy(11, false, false, true), queryContext));
    }

    @Test
    public void test2x()
    {
        RowBase[] expectedX = new RowBase[]{
        };
        compareRows(expectedX, cursor(intersectPxPy(22, true, true, false), queryContext));
        compareRows(expectedX, cursor(intersectPxPy(22, true, true, true), queryContext));
        reverse(expectedX);
        compareRows(expectedX, cursor(intersectPxPy(22, true, false, false), queryContext));
        compareRows(expectedX, cursor(intersectPxPy(22, true, false, true), queryContext));
        RowBase[] expectedY = new RowBase[]{
        };
        compareRows(expectedY, cursor(intersectPxPy(22, false, true, false), queryContext));
        compareRows(expectedY, cursor(intersectPxPy(22, false, true, true), queryContext));
        reverse(expectedY);
        compareRows(expectedY, cursor(intersectPxPy(22, false, false, false), queryContext));
        compareRows(expectedY, cursor(intersectPxPy(22, false, false, true), queryContext));
    }

    @Test
    public void test3x()
    {
        RowBase[] expectedX = new RowBase[]{
        };
        compareRows(expectedX, cursor(intersectPxPy(31, true, true, false), queryContext));
        compareRows(expectedX, cursor(intersectPxPy(31, true, true, true), queryContext));
        compareRows(expectedX, cursor(intersectPxPy(32, true, true, false), queryContext));
        compareRows(expectedX, cursor(intersectPxPy(32, true, true, true), queryContext));
        reverse(expectedX);
        compareRows(expectedX, cursor(intersectPxPy(31, true, false, false), queryContext));
        compareRows(expectedX, cursor(intersectPxPy(31, true, false, true), queryContext));
        compareRows(expectedX, cursor(intersectPxPy(32, true, false, false), queryContext));
        compareRows(expectedX, cursor(intersectPxPy(32, true, false, true), queryContext));
        RowBase[] expectedY = new RowBase[]{
        };
        compareRows(expectedY, cursor(intersectPxPy(31, false, true, false), queryContext));
        compareRows(expectedY, cursor(intersectPxPy(31, false, true, true), queryContext));
        compareRows(expectedY, cursor(intersectPxPy(32, false, true, false), queryContext));
        compareRows(expectedY, cursor(intersectPxPy(32, false, true, true), queryContext));
        reverse(expectedY);
        compareRows(expectedY, cursor(intersectPxPy(31, false, false, false), queryContext));
        compareRows(expectedY, cursor(intersectPxPy(31, false, false, true), queryContext));
        compareRows(expectedY, cursor(intersectPxPy(32, false, false, false), queryContext));
        compareRows(expectedY, cursor(intersectPxPy(32, false, false, true), queryContext));
    }

    @Test
    public void test4x()
    {
        RowBase[] expectedX = new RowBase[]{
            row(parentXIndexRowType, 44L, 44L, 4001L),
            row(parentXIndexRowType, 44L, 44L, 4002L),
        };
        compareRows(expectedX, cursor(intersectPxPy(44, true, true, false), queryContext));
        compareRows(expectedX, cursor(intersectPxPy(44, true, true, true), queryContext));
        reverse(expectedX);
        compareRows(expectedX, cursor(intersectPxPy(44, true, false, false), queryContext));
        compareRows(expectedX, cursor(intersectPxPy(44, true, false, true), queryContext));
        RowBase[] expectedY = new RowBase[]{
            row(parentYIndexRowType, 44L, 4001L),
            row(parentYIndexRowType, 44L, 4002L),
        };
        compareRows(expectedY, cursor(intersectPxPy(44, false, true, false), queryContext));
        compareRows(expectedY, cursor(intersectPxPy(44, false, true, true), queryContext));
        reverse(expectedY);
        compareRows(expectedY, cursor(intersectPxPy(44, false, false, false), queryContext));
        compareRows(expectedY, cursor(intersectPxPy(44, false, false, true), queryContext));
    }

    @Test
    public void test5x()
    {
        RowBase[] expectedX = new RowBase[]{
            row(parentXIndexRowType, 55L, 55L, 5001L),
            row(parentXIndexRowType, 55L, 55L, 5002L),
        };
        compareRows(expectedX, cursor(intersectPxPy(55, true, true, false), queryContext));
        compareRows(expectedX, cursor(intersectPxPy(55, true, true, true), queryContext));
        reverse(expectedX);
        compareRows(expectedX, cursor(intersectPxPy(55, true, false, false), queryContext));
        compareRows(expectedX, cursor(intersectPxPy(55, true, false, true), queryContext));
        RowBase[] expectedY = new RowBase[]{
            row(parentYIndexRowType, 55L, 5001L),
            row(parentYIndexRowType, 55L, 5002L),
        };
        compareRows(expectedY, cursor(intersectPxPy(55, false, true, false), queryContext));
        compareRows(expectedY, cursor(intersectPxPy(55, false, true, true), queryContext));
        reverse(expectedY);
        compareRows(expectedY, cursor(intersectPxPy(55, false, false, false), queryContext));
        compareRows(expectedY, cursor(intersectPxPy(55, false, false, true), queryContext));
    }

    @Test
    public void test6x()
    {
        RowBase[] expectedX = new RowBase[]{
            row(parentXIndexRowType, 66L, 66L, 6002L),
            row(parentXIndexRowType, 66L, 66L, 6003L),
        };
        compareRows(expectedX, cursor(intersectPxPy(66, true, true, false), queryContext));
        compareRows(expectedX, cursor(intersectPxPy(66, true, true, true), queryContext));
        reverse(expectedX);
        compareRows(expectedX, cursor(intersectPxPy(66, true, false, false), queryContext));
        compareRows(expectedX, cursor(intersectPxPy(66, true, false, true), queryContext));
        RowBase[] expectedY = new RowBase[]{
            row(parentYIndexRowType, 66L, 6002L),
            row(parentYIndexRowType, 66L, 6003L),
        };
        compareRows(expectedY, cursor(intersectPxPy(66, false, true, false), queryContext));
        compareRows(expectedY, cursor(intersectPxPy(66, false, true, true), queryContext));
        reverse(expectedY);
        compareRows(expectedY, cursor(intersectPxPy(66, false, false, false), queryContext));
        compareRows(expectedY, cursor(intersectPxPy(66, false, false, true), queryContext));
    }

    @Test
    public void test7x()
    {
        RowBase[] expected = new RowBase[]{
        };
        compareRows(expected, cursor(intersectPxCz(70, JoinType.INNER_JOIN, true, false), queryContext));
        compareRows(expected, cursor(intersectPxCz(70, JoinType.INNER_JOIN, true, true), queryContext));
        reverse(expected);
        compareRows(expected, cursor(intersectPxCz(70, JoinType.INNER_JOIN, false, false), queryContext));
        compareRows(expected, cursor(intersectPxCz(70, JoinType.INNER_JOIN, false, true), queryContext));
    }

    @Test
    public void test8x()
    {
        RowBase[] expected = new RowBase[]{
            row(childRowType, 88L, 8000L, 800000L),
            row(childRowType, 88L, 8001L, 800100L),
            row(childRowType, 88L, 8001L, 800101L),
            row(childRowType, 88L, 8002L, 800200L),
            row(childRowType, 88L, 8002L, 800201L),
            row(childRowType, 88L, 8002L, 800202L),
        };
        compareRows(expected, cursor(intersectPxCz(88, JoinType.INNER_JOIN, true, false), queryContext));
        compareRows(expected, cursor(intersectPxCz(88, JoinType.INNER_JOIN, true, true), queryContext));
        reverse(expected);
        compareRows(expected, cursor(intersectPxCz(88, JoinType.INNER_JOIN, false, false), queryContext));
        compareRows(expected, cursor(intersectPxCz(88, JoinType.INNER_JOIN, false, true), queryContext));
    }

    @Test
    public void test9x()
    {
        RowBase[] expected = new RowBase[]{
        };
        compareRows(expected, cursor(intersectPxCz(99, JoinType.INNER_JOIN, true, false), queryContext));
        compareRows(expected, cursor(intersectPxCz(99, JoinType.INNER_JOIN, true, true), queryContext));
        reverse(expected);
        compareRows(expected, cursor(intersectPxCz(99, JoinType.INNER_JOIN, false, false), queryContext));
        compareRows(expected, cursor(intersectPxCz(99, JoinType.INNER_JOIN, false, true), queryContext));
    }

    @Test
    public void test12x()
    {
        RowBase[] expected = new RowBase[]{
            row(childRowType, 12L, null, 1200000L),
        };
        compareRows(expected, cursor(intersectPxCz(12, JoinType.RIGHT_JOIN, true, false), queryContext));
        compareRows(expected, cursor(intersectPxCz(12, JoinType.RIGHT_JOIN, true, true), queryContext));
        reverse(expected);
        compareRows(expected, cursor(intersectPxCz(12, JoinType.RIGHT_JOIN, false, false), queryContext));
        compareRows(expected, cursor(intersectPxCz(12, JoinType.RIGHT_JOIN, false, true), queryContext));
    }

    @Test
    public void test13x()
    {
        RowBase[] expectedX = new RowBase[]{
            row(parentXIndexRowType, 13L, 13L, 13002L),
            row(parentXIndexRowType, 13L, 13L, 13005L),
            row(parentXIndexRowType, 13L, 13L, 13010L),
            row(parentXIndexRowType, 13L, 13L, 13011L),
            row(parentXIndexRowType, 13L, 13L, 13012L),
        };
        compareRows(expectedX, cursor(intersectPxPy(13, true, true, false), queryContext));
        compareRows(expectedX, cursor(intersectPxPy(13, true, true, true), queryContext));
        reverse(expectedX);
        compareRows(expectedX, cursor(intersectPxPy(13, true, false, false), queryContext));
        compareRows(expectedX, cursor(intersectPxPy(13, true, false, true), queryContext));
        RowBase[] expectedY = new RowBase[]{
            row(parentYIndexRowType, 13L, 13002L),
            row(parentYIndexRowType, 13L, 13005L),
            row(parentYIndexRowType, 13L, 13010L),
            row(parentYIndexRowType, 13L, 13011L),
            row(parentYIndexRowType, 13L, 13012L),
        };
        compareRows(expectedY, cursor(intersectPxPy(13, false, true, false), queryContext));
        compareRows(expectedY, cursor(intersectPxPy(13, false, true, true), queryContext));
        reverse(expectedY);
        compareRows(expectedY, cursor(intersectPxPy(13, false, false, false), queryContext));
        compareRows(expectedY, cursor(intersectPxPy(13, false, false, true), queryContext));
    }

    private Operator intersectPxPy(int key, boolean leftOutput, boolean ascending, boolean skipScan)
    {
        Operator plan =
            intersect_Ordered(
                indexScan_Default(
                    parentXIndexRowType,
                    parentXEq(key),
                    ordering(field(parentXIndexRowType, 1), ascending)),
                indexScan_Default(
                    parentYIndexRowType,
                    parentYEq(key),
                    ordering(field(parentYIndexRowType, 1), ascending)),
                parentXIndexRowType,
                parentYIndexRowType,
                1,
                1,
                ascending(ascending),
                JoinType.INNER_JOIN,
                EnumSet.of(skipScan
                           ? IntersectOption.SKIP_SCAN
                           : IntersectOption.SEQUENTIAL_SCAN,
                           leftOutput
                           ? IntersectOption.OUTPUT_LEFT
                           : IntersectOption.OUTPUT_RIGHT));
        return plan;
    }

    private Operator intersectPxCz(int key, JoinType joinType, boolean ascending, boolean skipScan)
    {
        Operator plan =
            intersect_Ordered(
                indexScan_Default(
                    parentXIndexRowType,
                    parentXEq(key),
                    ordering(field(parentXIndexRowType, 1), ascending)),
                indexScan_Default(
                    childZIndexRowType,
                    childZEq(key),
                    ordering(field(childZIndexRowType, 1), ascending,
                             field(childZIndexRowType, 2), ascending)),
                    parentXIndexRowType,
                    childZIndexRowType,
                    1,
                    2,
                    ascending(ascending),
                    joinType,
                    EnumSet.of(skipScan
                               ? IntersectOption.SKIP_SCAN
                               : IntersectOption.SEQUENTIAL_SCAN,
                               IntersectOption.OUTPUT_RIGHT));
        return plan;
    }

    private IndexKeyRange parentXEq(long x)
    {
        IndexBound xBound = new IndexBound(row(parentXIndexRowType, x), new SetColumnSelector(0));
        return IndexKeyRange.bounded(parentXIndexRowType, xBound, true, xBound, true);
    }

    private IndexKeyRange parentYEq(long y)
    {
        IndexBound yBound = new IndexBound(row(parentYIndexRowType, y), new SetColumnSelector(0));
        return IndexKeyRange.bounded(parentYIndexRowType, yBound, true, yBound, true);
    }

    private IndexKeyRange childZEq(long z)
    {
        IndexBound zBound = new IndexBound(row(childZIndexRowType, z), new SetColumnSelector(0));
        return IndexKeyRange.bounded(childZIndexRowType, zBound, true, zBound, true);
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
    
    private boolean[] ascending(boolean ... ascending)
    {
        return ascending;
    }

    private void reverse(RowBase[] rows)
    {
        int n = rows.length;
        for (int i = 0; i < n / 2; i++) {
            RowBase r = rows[i];
            rows[i] = rows[n - 1 - i];
            rows[n - 1 - i] = r;
        }
    }
}
