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
import com.akiban.qp.operator.ExpressionGenerator;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.rowtype.*;
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.expression.Expression;
import org.junit.Before;
import org.junit.Test;

import static com.akiban.qp.operator.API.*;
import static com.akiban.server.test.ExpressionGenerators.field;
import static junit.framework.Assert.fail;

// Single-branch testing. See MultiIndexCrossBranchIT for cross-branch testing.

public class HKeyUnion_OrderedIT extends OperatorITBase
{
    @Before
    public void before()
    {
        parent = createTable(
            "schema", "parent",
            "pid int not null primary key",
            "x int",
            "y int");
        createIndex("schema", "parent", "x", "x");
        createIndex("schema", "parent", "y", "y");
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
        parentXIndexRowType = indexType(parent, "x");
        parentYIndexRowType = indexType(parent, "y");
        childZIndexRowType = indexType(child, "z");
        coi = group(parent);
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
        db = new NewRow[]{
            // 0x: Both index scans empty
            // 1x: Left empty
            createNewRow(parent, 1000L, -1L, 12L),
            createNewRow(parent, 1001L, -1L, 12L),
            createNewRow(parent, 1002L, -1L, 12L),
            // 2x: Right empty
            createNewRow(parent, 2000L, 22L, -1L),
            createNewRow(parent, 2001L, 22L, -1L),
            createNewRow(parent, 2002L, 22L, -1L),
            // 3x: Both non-empty, and no overlap
            createNewRow(parent, 3000L, 31L, -1L),
            createNewRow(parent, 3001L, 31L, -1L),
            createNewRow(parent, 3002L, 31L, -1L),
            createNewRow(parent, 3003L, 9999L, 32L),
            createNewRow(parent, 3004L, 9999L, 32L),
            createNewRow(parent, 3005L, 9999L, 32L),
            // 4x: left contains right
            createNewRow(parent, 4000L, 44L, -1L),
            createNewRow(parent, 4001L, 44L, 44L),
            createNewRow(parent, 4002L, 44L, 44L),
            createNewRow(parent, 4003L, 44L, 9999L),
            // 5x: right contains left
            createNewRow(parent, 5000L, -1L, 55L),
            createNewRow(parent, 5001L, 55L, 55L),
            createNewRow(parent, 5002L, 55L, 55L),
            createNewRow(parent, 5003L, 9999L, 55L),
            // 6x: overlap but neither side contains the other
            createNewRow(parent, 6000L, -1L, 66L),
            createNewRow(parent, 6001L, -1L, 66L),
            createNewRow(parent, 6002L, 66L, 66L),
            createNewRow(parent, 6003L, 66L, 66L),
            createNewRow(parent, 6004L, 66L, 9999L),
            createNewRow(parent, 6005L, 66L, 9999L),
            // 7x: parent with no children
            createNewRow(parent, 7000L, 70L, 70L),
            // 8x: parent with children
            createNewRow(parent, 8000L, 88L, 88L),
            createNewRow(child, 800000L, 8000L, 88L),
            createNewRow(parent, 8001L, 88L, 88L),
            createNewRow(child, 800100L, 8001L, 88L),
            createNewRow(child, 800101L, 8001L, 88L),
            createNewRow(parent, 8002L, 88L, 88L),
            createNewRow(child, 800200L, 8002L, 88L),
            createNewRow(child, 800201L, 8002L, 88L),
            createNewRow(child, 800202L, 8002L, 88L),
            // 9x child with no parent
            createNewRow(child, 900000L, 9000L, 99L),
            // 12x right join (child on right)
            createNewRow(child, 1200000L, null, 12L),
        };
        use(db);
    }

    // IllegalArumentException tests

    @Test
    public void testInputNull()
    {
        try {
            hKeyUnion_Ordered(null,
                              groupScan_Default(coi),
                              parentXIndexRowType,
                              parentYIndexRowType,
                              1,
                              1,
                              1,
                              parentRowType);
        } catch (IllegalArgumentException e) {
        }
        try {
            hKeyUnion_Ordered(groupScan_Default(coi),
                              null,
                              parentXIndexRowType,
                              parentYIndexRowType,
                              1,
                              1,
                              1,
                              parentRowType);
            fail();
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void testRowTypesNull()
    {
        try {
            hKeyUnion_Ordered(groupScan_Default(coi),
                              groupScan_Default(coi),
                              null,
                              parentYIndexRowType,
                              1,
                              1,
                              1,
                              parentRowType);
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            hKeyUnion_Ordered(groupScan_Default(coi),
                              groupScan_Default(coi),
                              parentXIndexRowType,
                              null,
                              1,
                              1,
                              1,
                              parentRowType);
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            hKeyUnion_Ordered(groupScan_Default(coi),
                              groupScan_Default(coi),
                              parentXIndexRowType,
                              parentYIndexRowType,
                              1,
                              1,
                              1,
                              null);
            fail();
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void testOrderingColumns()
    {
        try {
            hKeyUnion_Ordered(groupScan_Default(coi),
                              groupScan_Default(coi),
                              parentXIndexRowType,
                              parentYIndexRowType,
                              -1,
                              1,
                              1,
                              parentRowType);
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            hKeyUnion_Ordered(groupScan_Default(coi),
                              groupScan_Default(coi),
                              parentXIndexRowType,
                              parentYIndexRowType,
                              3,
                              1,
                              1,
                              parentRowType);
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            hKeyUnion_Ordered(groupScan_Default(coi),
                              groupScan_Default(coi),
                              parentXIndexRowType,
                              parentYIndexRowType,
                              1,
                              -1,
                              1,
                              parentRowType);
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            hKeyUnion_Ordered(groupScan_Default(coi),
                              groupScan_Default(coi),
                              parentXIndexRowType,
                              parentYIndexRowType,
                              1,
                              3,
                              1,
                              parentRowType);
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            hKeyUnion_Ordered(groupScan_Default(coi),
                              groupScan_Default(coi),
                              parentXIndexRowType,
                              parentYIndexRowType,
                              1,
                              1,
                              -1,
                              parentRowType);
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            hKeyUnion_Ordered(groupScan_Default(coi),
                              groupScan_Default(coi),
                              parentXIndexRowType,
                              parentYIndexRowType,
                              1,
                              1,
                              2,
                              parentRowType);
            fail();
        } catch (IllegalArgumentException e) {
        }
    }

    // Runtime tests

    @Test
    public void test0x()
    {
        Operator plan = unionPxPy(0);
        String[] expected = new String[]{
        };
        compareRenderedHKeys(expected, cursor(plan, queryContext));
    }

    @Test
    public void test1x()
    {
        Operator plan = unionPxPy(12);
        String[] expected = new String[]{
            pKey(1000L),
            pKey(1001L),
            pKey(1002L),
        };
        compareRenderedHKeys(expected, cursor(plan, queryContext));
    }

    @Test
    public void test2x()
    {
        Operator plan = unionPxPy(22);
        String[] expected = new String[]{
            pKey(2000L),
            pKey(2001L),
            pKey(2002L),
        };
        compareRenderedHKeys(expected, cursor(plan, queryContext));
    }

    @Test
    public void test3x()
    {
        Operator plan = unionPxPy(31);
        String[] expected = new String[]{
            pKey(3000L),
            pKey(3001L),
            pKey(3002L),
        };
        compareRenderedHKeys(expected, cursor(plan, queryContext));
        plan = unionPxPy(32);
        expected = new String[]{
            pKey(3003L),
            pKey(3004L),
            pKey(3005L),
        };
        compareRenderedHKeys(expected, cursor(plan, queryContext));
    }

    @Test
    public void test4x()
    {
        Operator plan = unionPxPy(44);
        String[] expected = new String[]{
            pKey(4000L),
            pKey(4001L),
            pKey(4002L),
            pKey(4003L),
        };
        compareRenderedHKeys(expected, cursor(plan, queryContext));
    }

    @Test
    public void test5x()
    {
        Operator plan = unionPxPy(55);
        String[] expected = new String[]{
            pKey(5000L),
            pKey(5001L),
            pKey(5002L),
            pKey(5003L),
        };
        compareRenderedHKeys(expected, cursor(plan, queryContext));
    }

    @Test
    public void test6x()
    {
        Operator plan = unionPxPy(66);
        String[] expected = new String[]{
            pKey(6000L),
            pKey(6001L),
            pKey(6002L),
            pKey(6003L),
            pKey(6004L),
            pKey(6005L),
        };
        compareRenderedHKeys(expected, cursor(plan, queryContext));
    }

    @Test
    public void test7x()
    {
        Operator plan = unionPxCz(70);
        String[] expected = new String[]{
            pKey(7000L),
        };
        compareRenderedHKeys(expected, cursor(plan, queryContext));
    }

    @Test
    public void test8x()
    {
        Operator plan = unionPxCz(88);
        String[] expected = new String[]{
            pKey(8000L),
            pKey(8001L),
            pKey(8002L),
        };
        compareRenderedHKeys(expected, cursor(plan, queryContext));
    }

    @Test
    public void test9x()
    {
        Operator plan = unionPxCz(99);
        String[] expected = new String[]{
            pKey(9000L),
        };
        compareRenderedHKeys(expected, cursor(plan, queryContext));
    }

    @Test
    public void test12x()
    {
        Operator plan = unionPxCz(12);
        String[] expected = new String[]{
            pKey(null),
        };
        compareRenderedHKeys(expected, cursor(plan, queryContext));
    }

    @Test
    public void testCursor()
    {
        Operator plan = unionPxCz(88);
        CursorLifecycleTestCase testCase = new CursorLifecycleTestCase()
        {
            @Override
            public boolean hKeyComparison()
            {
                return true;
            }

            @Override
            public String[] firstExpectedHKeys()
            {
                return new String[] {
                    pKey(8000L),
                    pKey(8001L),
                    pKey(8002L),
                };
            }
        };
        testCursorLifecycle(plan, testCase);
    }

    private Operator unionPxPy(int key)
    {
        Operator plan =
            hKeyUnion_Ordered(
                indexScan_Default(
                    parentXIndexRowType,
                    parentXEq(key),
                    ordering(field(parentXIndexRowType, 1), true)),
                indexScan_Default(
                    parentYIndexRowType,
                    parentYEq(key),
                    ordering(field(parentYIndexRowType, 1), true)),
                parentXIndexRowType,
                parentYIndexRowType,
                1,
                1,
                1,
                parentRowType);
        return plan;
    }

    private Operator unionPxCz(int key)
    {
        Operator plan =
            hKeyUnion_Ordered(
                indexScan_Default(
                    parentXIndexRowType,
                    parentXEq(key),
                    ordering(field(parentXIndexRowType, 1), true)),
                indexScan_Default(
                    childZIndexRowType,
                    childZEq(key),
                    ordering(field(childZIndexRowType, 1), true,
                             field(childZIndexRowType, 2), true)),
                    parentXIndexRowType,
                    childZIndexRowType,
                    1,
                    2,
                    1,
                    parentRowType);
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
            ExpressionGenerator expression = (ExpressionGenerator) objects[i++];
            Boolean ascending = (Boolean) objects[i++];
            ordering.append(expression, ascending);
        }
        return ordering;
    }

    private String pKey(Long pid)
    {
        return String.format("{%d,%s}", parent, hKeyValue(pid));
    }

    private int parent;
    private int child;
    private UserTableRowType parentRowType;
    private UserTableRowType childRowType;
    private IndexRowType parentPidIndexRowType;
    private IndexRowType parentXIndexRowType;
    private IndexRowType parentYIndexRowType;
    private IndexRowType childZIndexRowType;
}
