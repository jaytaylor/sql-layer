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
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.std.FieldExpression;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static com.akiban.qp.operator.API.*;
import static com.akiban.server.expression.std.Expressions.field;
import static junit.framework.Assert.fail;

public class HKeyUnion_OrderedIT extends OperatorITBase
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
        child = createTable(
            "schema", "child",
            "cid int not null key",
            "pid int",
            "z int",
            "index(z)",
            "constraint __akiban_cp foreign key __akiban_cp(pid) references parent(pid)");
        schema = new Schema(rowDefCache().ais());
        parentRowType = schema.userTableRowType(userTable(parent));
        childRowType = schema.userTableRowType(userTable(child));
        parentPidIndexRowType = indexType(parent, "pid");
        parentXIndexRowType = indexType(parent, "x");
        parentYIndexRowType = indexType(parent, "y");
        childZIndexRowType = indexType(child, "z");
        hKeyRowType = schema.newHKeyRowType(parentRowType.userTable().hKey());
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
        RowBase[] expected = new RowBase[]{
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void test1x()
    {
        Operator plan = unionPxPy(12);
        RowBase[] expected = new RowBase[]{
            row(pKey(1000L), hKeyRowType),
            row(pKey(1001L), hKeyRowType),
            row(pKey(1002L), hKeyRowType),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void test2x()
    {
        Operator plan = unionPxPy(22);
        RowBase[] expected = new RowBase[]{
            row(pKey(2000L), hKeyRowType),
            row(pKey(2001L), hKeyRowType),
            row(pKey(2002L), hKeyRowType),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void test3x()
    {
        Operator plan = unionPxPy(31);
        RowBase[] expected = new RowBase[]{
            row(pKey(3000L), hKeyRowType),
            row(pKey(3001L), hKeyRowType),
            row(pKey(3002L), hKeyRowType),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = unionPxPy(32);
        expected = new RowBase[]{
            row(pKey(3003L), hKeyRowType),
            row(pKey(3004L), hKeyRowType),
            row(pKey(3005L), hKeyRowType),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void test4x()
    {
        Operator plan = unionPxPy(44);
        RowBase[] expected = new RowBase[]{
            row(pKey(4000L), hKeyRowType),
            row(pKey(4001L), hKeyRowType),
            row(pKey(4002L), hKeyRowType),
            row(pKey(4003L), hKeyRowType),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void test5x()
    {
        Operator plan = unionPxPy(55);
        RowBase[] expected = new RowBase[]{
            row(pKey(5000L), hKeyRowType),
            row(pKey(5001L), hKeyRowType),
            row(pKey(5002L), hKeyRowType),
            row(pKey(5003L), hKeyRowType),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void test6x()
    {
        Operator plan = unionPxPy(66);
        RowBase[] expected = new RowBase[]{
            row(pKey(6000L), hKeyRowType),
            row(pKey(6001L), hKeyRowType),
            row(pKey(6002L), hKeyRowType),
            row(pKey(6003L), hKeyRowType),
            row(pKey(6004L), hKeyRowType),
            row(pKey(6005L), hKeyRowType),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void test7x()
    {
        Operator plan = unionPxCz(70);
        RowBase[] expected = new RowBase[]{
            row(pKey(7000L), hKeyRowType),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void test8x()
    {
        Operator plan = unionPxCz(88);
        RowBase[] expected = new RowBase[]{
            row(pKey(8000L), hKeyRowType),
            row(pKey(8001L), hKeyRowType),
            row(pKey(8002L), hKeyRowType),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void test9x()
    {
        Operator plan = unionPxCz(99);
        RowBase[] expected = new RowBase[]{
            row(pKey(9000L), hKeyRowType),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void test12x()
    {
        Operator plan = unionPxCz(12);
        RowBase[] expected = new RowBase[]{
            row(pKey(null), hKeyRowType),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void testAllOrderingFieldsNoComparisonFields()
    {
        Operator plan =
            hKeyUnion_Ordered(
                indexScan_Default(parentPidIndexRowType),
                indexScan_Default(parentPidIndexRowType),
                parentPidIndexRowType,
                parentPidIndexRowType,
                1,
                1,
                0,
                parentRowType);
        RowBase[] expected = new RowBase[]{
            row(parentPidIndexRowType, 1000L),
            row(parentPidIndexRowType, 1001L),
            row(parentPidIndexRowType, 1002L),
            row(parentPidIndexRowType, 2000L),
            row(parentPidIndexRowType, 2001L),
            row(parentPidIndexRowType, 2002L),
            row(parentPidIndexRowType, 3000L),
            row(parentPidIndexRowType, 3001L),
            row(parentPidIndexRowType, 3002L),
            row(parentPidIndexRowType, 3003L),
            row(parentPidIndexRowType, 3004L),
            row(parentPidIndexRowType, 3005L),
            row(parentPidIndexRowType, 4000L),
            row(parentPidIndexRowType, 4001L),
            row(parentPidIndexRowType, 4002L),
            row(parentPidIndexRowType, 4003L),
            row(parentPidIndexRowType, 5000L),
            row(parentPidIndexRowType, 5001L),
            row(parentPidIndexRowType, 5002L),
            row(parentPidIndexRowType, 5003L),
            row(parentPidIndexRowType, 6000L),
            row(parentPidIndexRowType, 6001L),
            row(parentPidIndexRowType, 6002L),
            row(parentPidIndexRowType, 6003L),
            row(parentPidIndexRowType, 6004L),
            row(parentPidIndexRowType, 6005L),
            row(parentPidIndexRowType, 7000L),
            row(parentPidIndexRowType, 8000L),
            row(parentPidIndexRowType, 8001L),
            row(parentPidIndexRowType, 8002L),
        };
        compareRows(expected, cursor(plan, queryContext));
    }
    
    @Test
    public void testRowIntersection()
    {
        Operator parentProject =
            project_Default(
                filter_Default(
                    groupScan_Default(coi),
                    Collections.singleton(parentRowType)),
                parentRowType,
                Arrays.asList((Expression) new FieldExpression(parentRowType, 1),
                              (Expression) new FieldExpression(parentRowType, 2),
                              (Expression) new FieldExpression(parentRowType, 0)));
        Operator childProject =
            project_Default(
                filter_Default(
                    groupScan_Default(coi),
                    Collections.singleton(childRowType)),
                childRowType,
                Arrays.asList((Expression) new FieldExpression(childRowType, 2),
                              (Expression) new FieldExpression(childRowType, 1),
                              (Expression) new FieldExpression(childRowType, 0)));
        Operator plan =
            hKeyUnion_Ordered(
                parentProject,
                childProject,
                parentProject.rowType(),
                childProject.rowType(),
                1,
                2,
                1,
                parentRowType);
        RowBase[] expected = new RowBase[]{
            row(childRowType, 12L, null, 1200000L),
            row(childRowType, 88L, 8000L, 800000L),
            row(childRowType, 88L, 8001L, 800100L),
            row(childRowType, 88L, 8001L, 800101L),
            row(childRowType, 88L, 8002L, 800200L),
            row(childRowType, 88L, 8002L, 800201L),
            row(childRowType, 88L, 8002L, 800202L),
            row(childRowType, 99L, 9000L, 900000L),
        };
        compareRows(expected, cursor(plan, queryContext));
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
            Expression expression = (Expression) objects[i++];
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
    private RowType hKeyRowType;
}
