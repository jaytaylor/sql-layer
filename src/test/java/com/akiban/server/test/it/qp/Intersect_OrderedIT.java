/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
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

import static com.akiban.qp.operator.API.*;
import static com.akiban.server.expression.std.Expressions.field;

public class Intersect_OrderedIT extends OperatorITBase
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
        parentXIndexRowType = indexType(parent, "x");
        parentYIndexRowType = indexType(parent, "y");
        childZIndexRowType = indexType(child, "z");
        coi = groupTable(parent);
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
            // 11x left join (parent on left)
            createNewRow(parent, 11000L, 11L, 11L),
            // 12x right join (parent on left)
            createNewRow(child, 1200000L, null, 12L),
            // 13x full join
            createNewRow(parent, 13000L, 13L, 13L),
            createNewRow(child, 1300000L, 13999L, 13L),
        };
        use(db);
    }

    private int parent;
    private int child;
    private RowType parentRowType;
    private RowType childRowType;
    private IndexRowType parentXIndexRowType;
    private IndexRowType parentYIndexRowType;
    private IndexRowType childZIndexRowType;

    // IllegalArumentException tests

    @Test(expected = IllegalArgumentException.class)
    public void testLeftInputNull()
    {
        intersect_Ordered(null,
                          groupScan_Default(coi),
                          parentXIndexRowType,
                          parentYIndexRowType,
                          1,
                          1,
                          JoinType.INNER_JOIN,
                          0,
                          1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRightInputNull()
    {
        intersect_Ordered(groupScan_Default(coi),
                          null,
                          parentXIndexRowType,
                          parentYIndexRowType,
                          1,
                          1,
                          JoinType.INNER_JOIN,
                          0,
                          1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLeftIndexRowTypeNull()
    {
        intersect_Ordered(groupScan_Default(coi),
                          groupScan_Default(coi),
                          null,
                          parentYIndexRowType,
                          1,
                          1,
                          JoinType.INNER_JOIN,
                          0,
                          1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRightIndexRowTypeNull()
    {
        intersect_Ordered(groupScan_Default(coi),
                          groupScan_Default(coi),
                          parentXIndexRowType,
                          null,
                          1,
                          1,
                          JoinType.INNER_JOIN,
                          0,
                          1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRightJoinTypeNull()
    {
        intersect_Ordered(groupScan_Default(coi),
                          groupScan_Default(coi),
                          parentXIndexRowType,
                          parentYIndexRowType,
                          1,
                          1,
                          null,
                          0,
                          1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoOrderingColumns()
    {
        intersect_Ordered(groupScan_Default(coi),
                          groupScan_Default(coi),
                          parentXIndexRowType,
                          parentYIndexRowType,
                          0,
                          0,
                          JoinType.INNER_JOIN,
                          0,
                          1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEnoughLeftColumns()
    {
        intersect_Ordered(groupScan_Default(coi),
                          groupScan_Default(coi),
                          parentXIndexRowType,
                          parentYIndexRowType,
                          3,
                          1,
                          JoinType.INNER_JOIN,
                          0,
                          1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEnoughRightColumns()
    {
        intersect_Ordered(groupScan_Default(coi),
                          groupScan_Default(coi),
                          parentXIndexRowType,
                          parentYIndexRowType,
                          1,
                          3,
                          JoinType.INNER_JOIN,
                          0,
                          1);
    }

    // Runtime tests

    @Test
    public void test0x()
    {
        Operator plan = intersectPxPy(0);
        RowBase[] expected = new RowBase[]{
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void test1x()
    {
        Operator plan = intersectPxPy(12);
        RowBase[] expected = new RowBase[]{
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void test2x()
    {
        Operator plan = intersectPxPy(22);
        RowBase[] expected = new RowBase[]{
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void test3x()
    {
        Operator plan = intersectPxPy(31);
        RowBase[] expected = new RowBase[]{
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = intersectPxPy(32);
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void test4x()
    {
        Operator plan = intersectPxPy(44);
        RowBase[] expected = new RowBase[]{
            row(intersectRowType, 44L, 4001L, 44L, 4001L),
            row(intersectRowType, 44L, 4002L, 44L, 4002L),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void test5x()
    {
        Operator plan = intersectPxPy(55);
        RowBase[] expected = new RowBase[]{
            row(intersectRowType, 55L, 5001L, 55L, 5001L),
            row(intersectRowType, 55L, 5002L, 55L, 5002L),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void test6x()
    {
        Operator plan = intersectPxPy(66);
        RowBase[] expected = new RowBase[]{
            row(intersectRowType, 66L, 6002L, 66L, 6002L),
            row(intersectRowType, 66L, 6003L, 66L, 6003L),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void test7x()
    {
        Operator plan = intersectPxCz(70);
        RowBase[] expected = new RowBase[]{
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void test8x()
    {
        Operator plan = intersectPxCz(88);
        RowBase[] expected = new RowBase[]{
            row(intersectRowType, 88L, 8000L, 88L, 8000L, 800000L),
            row(intersectRowType, 88L, 8001L, 88L, 8001L, 800100L),
            row(intersectRowType, 88L, 8001L, 88L, 8001L, 800101L),
            row(intersectRowType, 88L, 8002L, 88L, 8002L, 800200L),
            row(intersectRowType, 88L, 8002L, 88L, 8002L, 800201L),
            row(intersectRowType, 88L, 8002L, 88L, 8002L, 800202L),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void test9x()
    {
        Operator plan = intersectPxCz(99);
        RowBase[] expected = new RowBase[]{
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void test11x()
    {
        Operator plan = intersectPxCz(11, JoinType.LEFT_JOIN);
        RowBase[] expected = new RowBase[]{
            row(intersectRowType, 11L, 11000L, null, null, null),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void test12x()
    {
        Operator plan = intersectPxCz(12, JoinType.RIGHT_JOIN);
        RowBase[] expected = new RowBase[]{
            row(intersectRowType, null, null, 12L, null, 1200000L),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void test13x()
    {
        Operator plan = intersectPxCz(13, JoinType.FULL_JOIN);
        RowBase[] expected = new RowBase[]{
            row(intersectRowType, 13L, 13000L, null, null, null),
            row(intersectRowType, null, null, 13L, 13999L, 1300000L),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    private Operator intersectPxPy(int key)
    {
        Operator plan =
            intersect_Ordered(
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
                JoinType.INNER_JOIN,
                0,
                1);
        intersectRowType = plan.rowType();
        return plan;
    }

    private Operator intersectPxCz(int key)
    {
        return intersectPxCz(key, JoinType.INNER_JOIN);
    }
    
    private Operator intersectPxCz(int key, JoinType joinType)
    {
        Operator plan =
            intersect_Ordered(
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
                    joinType,
                    0,
                    1);
        intersectRowType = plan.rowType();
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

    private RowType intersectRowType;
}
