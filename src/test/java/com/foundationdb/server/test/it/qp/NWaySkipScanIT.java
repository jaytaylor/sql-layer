/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.test.it.qp;

import com.foundationdb.qp.expression.IndexBound;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.api.dml.SetColumnSelector;
import org.junit.Test;

import java.util.EnumSet;

import static com.foundationdb.qp.operator.API.*;
import static com.foundationdb.server.test.ExpressionGenerators.field;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class NWaySkipScanIT extends OperatorITBase
{
    @Override
    protected void setupCreateSchema()
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
    }

    @Override
    protected void setupPostCreateSchema()
    {
        schema = new Schema(ais());
        tRowType = schema.tableRowType(table(t));
        tIdIndexRowType = indexType(t, "id");
        tXIndexRowType = indexType(t, "x");
        tYIndexRowType = indexType(t, "y");
        tZIndexRowType = indexType(t, "z");
        adapter = newStoreAdapter(schema);
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
        db = new Row[] {
            row(t, 1000L, 71L, 81L, 91L),
            row(t, 1001L, 71L, 81L, 92L),
            row(t, 1002L, 71L, 82L, 91L),
            row(t, 1003L, 71L, 82L, 92L),
            row(t, 1004L, 72L, 81L, 91L),
            row(t, 1005L, 72L, 81L, 92L),
            row(t, 1006L, 72L, 82L, 91L),
            row(t, 1007L, 72L, 82L, 92L),
            row(t, 1008L, 73L, null, null),
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
        Row[] expected = new Row[] {
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
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testIntersectThenUnion()
    {
        Row[] expected = new Row[] {
            row(tXIndexRowType, 71L, 1000L),
            row(tXIndexRowType, 71L, 1001L),
            row(tXIndexRowType, 72L, 1004L),
            row(tXIndexRowType, 72L, 1005L),
            row(tXIndexRowType, 72L, 1006L),
            row(tXIndexRowType, 72L, 1007L),
        };
        compareRows(expected, cursor(intersectXYunionX(71, 81, 72, false), queryContext, queryBindings));
        compareRows(expected, cursor(intersectXYunionX(71, 81, 72, true), queryContext, queryBindings));
    }

    @Test
    public void testIntersectWithEmptyInputThenUnion()
    {
        Row[] expected = new Row[] {
            row(tXIndexRowType, 72L, 1004L),
            row(tXIndexRowType, 72L, 1005L),
            row(tXIndexRowType, 72L, 1006L),
            row(tXIndexRowType, 72L, 1007L),
        };
        // Left input to intersection is empty
        {
            compareRows(expected, cursor(intersectXYunionX(99, 81, 72, false), queryContext, queryBindings));
            compareRows(expected, cursor(intersectXYunionX(99, 81, 72, true), queryContext, queryBindings));
        }
        // Right input to intersection is empty
        {
            compareRows(expected, cursor(intersectXYunionX(71, 99, 72, false), queryContext, queryBindings));
            compareRows(expected, cursor(intersectXYunionX(71, 99, 72, true), queryContext, queryBindings));
        }
        // Both inputs to intersection are empty
        {
            compareRows(expected, cursor(intersectXYunionX(99, 99, 72, false), queryContext, queryBindings));
            compareRows(expected, cursor(intersectXYunionX(99, 99, 72, true), queryContext, queryBindings));
        }
    }

    @Test
    public void testUnionThenIntersect()
    {
        {
            Row[] expected = {
                row(tXIndexRowType, 71, 1000L),
                row(tXIndexRowType, 71, 1001L),
                row(tXIndexRowType, 72, 1004L),
                row(tXIndexRowType, 72, 1005L),
            };
            compareRows(expected, cursor(unionXXintersectY(71, 72, 81, LEFT, false), queryContext, queryBindings));
            compareRows(expected, cursor(unionXXintersectY(71, 72, 81, LEFT, true), queryContext, queryBindings));
        }
        {
            Row[] expected = {
                row(tXIndexRowType, 81, 1000L),
                row(tXIndexRowType, 81, 1001L),
                row(tXIndexRowType, 81, 1004L),
                row(tXIndexRowType, 81, 1005L),
            };
            compareRows(expected, cursor(unionXXintersectY(71, 72, 81, RIGHT, false), queryContext, queryBindings));
            compareRows(expected, cursor(unionXXintersectY(71, 72, 81, RIGHT, true), queryContext, queryBindings));
        }
        {
            Row[] expected = {
                row(tXIndexRowType, 71, 1002L),
                row(tXIndexRowType, 71, 1003L),
                row(tXIndexRowType, 72, 1006L),
                row(tXIndexRowType, 72, 1007L),
            };
            compareRows(expected, cursor(unionXXintersectY(71, 72, 82, LEFT, false), queryContext, queryBindings));
            compareRows(expected, cursor(unionXXintersectY(71, 72, 82, LEFT, true), queryContext, queryBindings));
        }
        {
            Row[] expected = {
                row(tXIndexRowType, 82, 1002L),
                row(tXIndexRowType, 82, 1003L),
                row(tXIndexRowType, 82, 1006L),
                row(tXIndexRowType, 82, 1007L),
            };
            compareRows(expected, cursor(unionXXintersectY(71, 72, 82, RIGHT, false), queryContext, queryBindings));
            compareRows(expected, cursor(unionXXintersectY(71, 72, 82, RIGHT, true), queryContext, queryBindings));
        }
    }

    @Test
    public void testUnionWithEmptyInputThenIntersect()
    {
        // Left input to union is empty
        {
            Row[] expected = {
                row(tXIndexRowType, 72, 1004L),
                row(tXIndexRowType, 72, 1005L),
            };
            compareRows(expected, cursor(unionXXintersectY(99, 72, 81, LEFT, false), queryContext, queryBindings));
            compareRows(expected, cursor(unionXXintersectY(99, 72, 81, LEFT, true), queryContext, queryBindings));
        }
        // Right input to union is empty
        {
            Row[] expected = {
                row(tXIndexRowType, 71, 1000L),
                row(tXIndexRowType, 71, 1001L),
            };
            compareRows(expected, cursor(unionXXintersectY(71, 99, 81, LEFT, false), queryContext, queryBindings));
            compareRows(expected, cursor(unionXXintersectY(71, 99, 81, LEFT, true), queryContext, queryBindings));
        }
        // Both inputs to union are empty
        {
            Row[] expected = {
            };
            compareRows(expected, cursor(unionXXintersectY(99, 99, 81, LEFT, false), queryContext, queryBindings));
            compareRows(expected, cursor(unionXXintersectY(99, 99, 81, LEFT, true), queryContext, queryBindings));
        }
    }

    private void testTwoIntersects(long key, Operator plan)
    {
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        cursor.openTopLevel();
        Row row = cursor.next();
        assertEquals(Long.valueOf(key), getLong(row, 0));
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
                        EnumSet.of(scanType, xyOutput),
                        null,
                        true),
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
                EnumSet.of(scanType, xyzOutput),
                null,
                true);
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
                ascending(true),
                false),
            indexScan_Default(
                tXIndexRowType,
                xEq(x3),
                ordering),
            tXIndexRowType,
            tXIndexRowType,
            1,
            1,
            ascending(true),
            false);
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
                    EnumSet.of(scanType, LEFT),
                    null,
                    true),
            indexScan_Default(
                tXIndexRowType,
                xEq(x2),
                xOrdering),
            tXIndexRowType,
            tXIndexRowType,
            1,
            1,
            ascending(true),
            false);
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
                        ascending(true),
                        false),
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
                EnumSet.of(scanType, intersectOutput),
                null,
                true);
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

    private boolean[] ascending(boolean... ascending)
    {
        return ascending;
    }
}
