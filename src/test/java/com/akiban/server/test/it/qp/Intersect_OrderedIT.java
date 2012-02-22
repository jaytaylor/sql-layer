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
            "pid int not null key",
            "x int",
            "y int",
            "index(x)",
            "index(y)");
        schema = new Schema(rowDefCache().ais());
        parentRowType = schema.userTableRowType(userTable(parent));
        parentXIndexRowType = indexType(parent, "x");
        parentYIndexRowType = indexType(parent, "y");
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
        };
        use(db);
    }

    private int parent;
    private RowType parentRowType;
    private IndexRowType parentXIndexRowType;
    private IndexRowType parentYIndexRowType;

    // IllegalArumentException tests

    @Test(expected = IllegalArgumentException.class)
    public void testLeftInputNull()
    {
        intersect_Ordered(null,
                          groupScan_Default(coi),
                          parentXIndexRowType,
                          parentYIndexRowType,
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
                          1, JoinType.INNER_JOIN,
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
                          1, JoinType.INNER_JOIN,
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
                          1, null,
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
                          0, JoinType.INNER_JOIN,
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
                          3, JoinType.INNER_JOIN,
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
                          3, JoinType.INNER_JOIN,
                          0,
                          1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testJoinTypeNotInner()
    {
        intersect_Ordered(groupScan_Default(coi),
                          groupScan_Default(coi),
                          parentXIndexRowType,
                          parentYIndexRowType,
                          1, JoinType.LEFT_JOIN,
                          0,
                          1);
    }

    // Runtime tests

    @Test
    public void test0x()
    {
        Operator plan = intersect(0);
        RowBase[] expected = new RowBase[]{
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void test1x()
    {
        Operator plan = intersect(12);
        RowBase[] expected = new RowBase[]{
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void test2x()
    {
        Operator plan = intersect(22);
        RowBase[] expected = new RowBase[]{
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void test3x()
    {
        Operator plan = intersect(31);
        RowBase[] expected = new RowBase[]{
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = intersect(32);
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void test4x()
    {
        Operator plan = intersect(44);
        RowBase[] expected = new RowBase[]{
            row(intersectRowType, 44L, 4001L, 44L, 4001L),
            row(intersectRowType, 44L, 4002L, 44L, 4002L),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void test5x()
    {
        Operator plan = intersect(55);
        RowBase[] expected = new RowBase[]{
            row(intersectRowType, 55L, 5001L, 55L, 5001L),
            row(intersectRowType, 55L, 5002L, 55L, 5002L),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void test6x()
    {
        Operator plan = intersect(66);
        RowBase[] expected = new RowBase[]{
            row(intersectRowType, 66L, 6002L, 66L, 6002L),
            row(intersectRowType, 66L, 6003L, 66L, 6003L),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    private Operator intersect(int key)
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
                JoinType.INNER_JOIN,
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
