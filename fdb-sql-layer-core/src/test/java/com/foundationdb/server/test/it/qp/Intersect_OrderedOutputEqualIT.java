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
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.ExpressionGenerator;
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
import static org.junit.Assert.fail;

// Single-branch testing. See MultiIndexCrossBranchIT for cross-branch testing.

public class Intersect_OrderedOutputEqualIT extends OperatorITBase
{
    @Override
    protected void setupCreateSchema()
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
        alien = createTable(
            "schema", "alien",
            "aid int not null primary key");
        createIndex("schema", "child", "z", "z");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        schema = new Schema(ais());
        parentRowType = schema.tableRowType(table(parent));
        childRowType = schema.tableRowType(table(child));
        parentPidIndexRowType = indexType(parent, "pid");
        parentXIndexRowType = indexType(parent, "x");
        parentYIndexRowType = indexType(parent, "y");
        childZIndexRowType = indexType(child, "z");
        alienAidIndexRowType = indexType(alien, "aid");
        coi = group(parent);
        adapter = newStoreAdapter(schema);
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
        db = new Row[]{
            // 0x: Both index scans empty
            // 1x: Left empty
            row(parent, 1000L, -1L, 11L),
            row(parent, 1001L, -1L, 11L),
            row(parent, 1002L, -1L, 11L),
            // 2x: Right empty
            row(parent, 2000L, 22L, -1L),
            row(parent, 2001L, 22L, -1L),
            row(parent, 2002L, 22L, -1L),
            // 3x: Both non-empty, and no overlap
            row(parent, 3000L, 31L, -1L),
            row(parent, 3001L, 31L, -1L),
            row(parent, 3002L, 31L, -1L),
            row(parent, 3003L, 9999L, 32L),
            row(parent, 3004L, 9999L, 32L),
            row(parent, 3005L, 9999L, 32L),
            // 4x: left contains right
            row(parent, 4000L, 44L, -1L),
            row(parent, 4001L, 44L, 44L),
            row(parent, 4002L, 44L, 44L),
            row(parent, 4003L, 44L, 9999L),
            // 5x: right contains left
            row(parent, 5000L, -1L, 55L),
            row(parent, 5001L, 55L, 55L),
            row(parent, 5002L, 55L, 55L),
            row(parent, 5003L, 9999L, 55L),
            // 6x: overlap but neither side contains the other
            row(parent, 6000L, -1L, 66L),
            row(parent, 6001L, -1L, 66L),
            row(parent, 6002L, 66L, 66L),
            row(parent, 6003L, 66L, 66L),
            row(parent, 6004L, 66L, 9999L),
            row(parent, 6005L, 66L, 9999L),
            // 7x: parent with no children
            row(parent, 7000L, 70L, 70L),
            // 8x: parent with children
            row(parent, 8000L, 88L, 88L),
            row(child, 800000L, 8000L, 88L),
            row(parent, 8001L, 88L, 88L),
            row(child, 800100L, 8001L, 88L),
            row(child, 800101L, 8001L, 88L),
            row(parent, 8002L, 88L, 88L),
            row(child, 800200L, 8002L, 88L),
            row(child, 800201L, 8002L, 88L),
            row(child, 800202L, 8002L, 88L),
            // 9x child with no parent
            row(parent, 9000L, 99L, 99L),
            row(child, 900100L, 9001L, 99L),
            row(parent, 9002L, 99L, 99L),
            row(child, 900300L, 9003L, 99L),
            // 12x right join (child on right)
            row(child, 1200000L, null, 12L),
        };
        use(db);
    }

    private int parent;
    private int child;
    private int alien;
    private RowType parentRowType;
    private RowType childRowType;
    private IndexRowType parentPidIndexRowType;
    private IndexRowType parentXIndexRowType;
    private IndexRowType parentYIndexRowType;
    private IndexRowType childZIndexRowType;
    private IndexRowType alienAidIndexRowType;

    // IllegalArumentException tests

    @Test
    public void testInputNull()
    {
        // First input null
        try {
            intersect_Ordered(null,
                    groupScan_Default(coi),
                    parentXIndexRowType,
                    parentYIndexRowType,
                    1,
                    1,
                    ascending(true),
                    JoinType.INNER_JOIN,
                    EnumSet.of(IntersectOption.OUTPUT_LEFT),
                    null, true);
        } catch (IllegalArgumentException e) {
        }
        // Second input null
        try {
            intersect_Ordered(groupScan_Default(coi),
                    null,
                    parentXIndexRowType,
                    parentYIndexRowType,
                    1,
                    1,
                    ascending(true),
                    JoinType.INNER_JOIN,
                    EnumSet.of(IntersectOption.OUTPUT_LEFT),
                    null, true);
            fail();
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void testInputType() {
        // First input type null
        try {
            intersect_Ordered(groupScan_Default(coi),
                    groupScan_Default(coi),
                    null,
                    parentYIndexRowType,
                    1,
                    1,
                    ascending(true),
                    JoinType.INNER_JOIN,
                    EnumSet.of(IntersectOption.OUTPUT_LEFT),
                    null, true);
            fail();
        } catch (IllegalArgumentException e) {
        }
        // Second input type null
        try {
            intersect_Ordered(groupScan_Default(coi),
                    groupScan_Default(coi),
                    parentXIndexRowType,
                    null,
                    1,
                    1,
                    ascending(true),
                    JoinType.INNER_JOIN,
                    EnumSet.of(IntersectOption.OUTPUT_LEFT),
                    null, true);
            fail();
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void testJoinType()
    {
        // join type null
        try {
            intersect_Ordered(groupScan_Default(coi),
                    groupScan_Default(coi),
                    parentXIndexRowType,
                    parentYIndexRowType,
                    1,
                    1,
                    ascending(true),
                    null,
                    EnumSet.of(IntersectOption.OUTPUT_LEFT),
                    null, true);
        } catch (IllegalArgumentException e) {
        }
        // full join not allowed
        try {
            intersect_Ordered(groupScan_Default(coi),
                    groupScan_Default(coi),
                    parentXIndexRowType,
                    parentYIndexRowType,
                    1,
                    1,
                    ascending(true),
                    JoinType.FULL_JOIN,
                    EnumSet.of(IntersectOption.OUTPUT_LEFT),
                    null, true);
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void testOutputOptionNull()
    {
        // output option null
        try {
            intersect_Ordered(groupScan_Default(coi),
                    groupScan_Default(coi),
                    parentXIndexRowType,
                    parentYIndexRowType,
                    1,
                    1,
                    ascending(true),
                    JoinType.INNER_JOIN,
                    null,
                    null, true);
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void testJoinTypeAndOrderingConsistency()
    {
        // These are OK
        intersect_Ordered(groupScan_Default(coi),
                groupScan_Default(coi),
                parentXIndexRowType,
                parentYIndexRowType,
                1,
                1,
                ascending(true),
                JoinType.INNER_JOIN,
                EnumSet.of(IntersectOption.OUTPUT_LEFT),
                null, true);
        intersect_Ordered(groupScan_Default(coi),
                groupScan_Default(coi),
                parentXIndexRowType,
                parentYIndexRowType,
                1,
                1,
                ascending(true),
                JoinType.INNER_JOIN,
                EnumSet.of(IntersectOption.OUTPUT_RIGHT),
                null, true);
        intersect_Ordered(groupScan_Default(coi),
                groupScan_Default(coi),
                parentXIndexRowType,
                parentYIndexRowType,
                1,
                1,
                ascending(true),
                JoinType.LEFT_JOIN,
                EnumSet.of(IntersectOption.OUTPUT_LEFT),
                null, true);
        // left join and output right are incompatible
        try {
            intersect_Ordered(groupScan_Default(coi),
                    groupScan_Default(coi),
                    parentXIndexRowType,
                    parentYIndexRowType,
                    1,
                    1,
                    ascending(true),
                    JoinType.LEFT_JOIN,
                    EnumSet.of(IntersectOption.OUTPUT_RIGHT),
                    null, true);
            fail();
        } catch (IllegalArgumentException e) {
        }
        // right join and output left are incompatible
        try {
            intersect_Ordered(groupScan_Default(coi),
                    groupScan_Default(coi),
                    parentXIndexRowType,
                    parentYIndexRowType,
                    1,
                    1,
                    ascending(true),
                    JoinType.RIGHT_JOIN,
                    EnumSet.of(IntersectOption.OUTPUT_LEFT),
                    null, true);
            fail();
        } catch (IllegalArgumentException e) {
        }
        // OK
        intersect_Ordered(groupScan_Default(coi),
                groupScan_Default(coi),
                parentXIndexRowType,
                parentYIndexRowType,
                1,
                1,
                ascending(true),
                JoinType.RIGHT_JOIN,
                EnumSet.of(IntersectOption.OUTPUT_RIGHT),
                null, true);
    }

    @Test
    public void testOrderingColumns()
    {
        // left ordering columns can't be negative
        try {
            intersect_Ordered(groupScan_Default(coi),
                    groupScan_Default(coi),
                    parentXIndexRowType,
                    parentYIndexRowType,
                    -1,
                    1,
                    ascending(true),
                    JoinType.INNER_JOIN,
                    EnumSet.of(IntersectOption.OUTPUT_LEFT),
                    null, true);
            fail();
        } catch (IllegalArgumentException e) {
        }
        // left ordering columns > columns in index
        try {
            intersect_Ordered(groupScan_Default(coi),
                    groupScan_Default(coi),
                    parentXIndexRowType,
                    parentYIndexRowType,
                    3,
                    1,
                    ascending(true),
                    JoinType.INNER_JOIN,
                    EnumSet.of(IntersectOption.OUTPUT_LEFT),
                    null, true);
            fail();
        } catch (IllegalArgumentException e) {
        }
        // right ordering columns can't be negative.
        try {
            intersect_Ordered(groupScan_Default(coi),
                    groupScan_Default(coi),
                    parentXIndexRowType,
                    parentYIndexRowType,
                    1,
                    -1,
                    ascending(true),
                    JoinType.INNER_JOIN,
                    EnumSet.of(IntersectOption.OUTPUT_LEFT),
                    null, true);
            fail();
        } catch (IllegalArgumentException e) {
        }
        // right ordering columns > columns in index
        try {
            intersect_Ordered(groupScan_Default(coi),
                    groupScan_Default(coi),
                    parentXIndexRowType,
                    parentYIndexRowType,
                    1,
                    3,
                    ascending(true),
                    JoinType.INNER_JOIN,
                    EnumSet.of(IntersectOption.OUTPUT_LEFT),
                    null, true);
            fail();
        } catch (IllegalArgumentException e) {
        }
        // comparison fields negative
        try {
            intersect_Ordered(groupScan_Default(coi),
                    groupScan_Default(coi),
                    parentXIndexRowType,
                    parentYIndexRowType,
                    1,
                    1,
                    -1,
                    JoinType.INNER_JOIN,
                    IntersectOption.OUTPUT_LEFT,
                    null, true);
            fail();
        } catch (IllegalArgumentException e) {
        }
        // ascending array too big
        try {
            intersect_Ordered(groupScan_Default(coi),
                    groupScan_Default(coi),
                    parentXIndexRowType,
                    parentYIndexRowType,
                    1,
                    1,
                    ascending(true, true),
                    JoinType.INNER_JOIN,
                    EnumSet.of(IntersectOption.OUTPUT_LEFT),
                    null, true);
            fail();
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void testOptions()
    {
        // No output option
        try {
            intersect_Ordered(groupScan_Default(coi),
                    groupScan_Default(coi),
                    parentXIndexRowType,
                    parentYIndexRowType,
                    1,
                    1,
                    ascending(true),
                    JoinType.INNER_JOIN,
                    EnumSet.of(IntersectOption.SEQUENTIAL_SCAN),
                    null, true);
            fail();
        } catch (IllegalArgumentException e) {
        }
        // Two output options
        try {
            intersect_Ordered(groupScan_Default(coi),
                    groupScan_Default(coi),
                    parentXIndexRowType,
                    parentYIndexRowType,
                    1,
                    1,
                    ascending(true),
                    JoinType.INNER_JOIN,
                    EnumSet.of(IntersectOption.OUTPUT_LEFT,
                            IntersectOption.SEQUENTIAL_SCAN,
                            IntersectOption.SKIP_SCAN),
                    null, true);
            fail();
        } catch (IllegalArgumentException e) {
        }
        // No scan option
        try {
            intersect_Ordered(groupScan_Default(coi),
                    groupScan_Default(coi),
                    parentXIndexRowType,
                    parentYIndexRowType,
                    1,
                    1,
                    ascending(true),
                    JoinType.INNER_JOIN,
                    EnumSet.of(IntersectOption.OUTPUT_LEFT),
                    null, true);
            // OK for now, see comment in Intersect_Ordered constructor. fail();
        } catch (IllegalArgumentException e) {
        }
        // Two scan options
        try {
            intersect_Ordered(groupScan_Default(coi),
                    groupScan_Default(coi),
                    parentXIndexRowType,
                    parentYIndexRowType,
                    1,
                    1,
                    ascending(true),
                    JoinType.INNER_JOIN,
                    EnumSet.of(IntersectOption.OUTPUT_LEFT,
                            IntersectOption.SEQUENTIAL_SCAN,
                            IntersectOption.SKIP_SCAN),
                    null, true);
            fail();
        } catch (IllegalArgumentException e) {
        }
    }

    // Runtime tests

    @Test
    public void test0x()
    {
        Row[] expected = new Row[]{
        };
        compareRows(expected, cursor(intersectPxPy(0, true, false), queryContext, queryBindings));
        compareRows(expected, cursor(intersectPxPy(0, true, true), queryContext, queryBindings));
        reverse(expected);
        compareRows(expected, cursor(intersectPxPy(0, false, false), queryContext, queryBindings));
        compareRows(expected, cursor(intersectPxPy(0, false, true), queryContext, queryBindings));
    }
    
    @Test
    public void test1x()
    {
        Row[] expected = new Row[]{
        };
        compareRows(expected, cursor(intersectPxPy(11, true, false), queryContext, queryBindings));
        compareRows(expected, cursor(intersectPxPy(11, true, true), queryContext, queryBindings));
        reverse(expected);
        compareRows(expected, cursor(intersectPxPy(11, false, false), queryContext, queryBindings));
        compareRows(expected, cursor(intersectPxPy(11, false, true), queryContext, queryBindings));
    }

    @Test
    public void test2x()
    {
        Row[] expected = new Row[]{
        };
        compareRows(expected, cursor(intersectPxPy(22, true, false), queryContext, queryBindings));
        compareRows(expected, cursor(intersectPxPy(22, true, true), queryContext, queryBindings));
        reverse(expected);
        compareRows(expected, cursor(intersectPxPy(22, false, false), queryContext, queryBindings));
        compareRows(expected, cursor(intersectPxPy(22, false, true), queryContext, queryBindings));
    }

    @Test
    public void test3x()
    {
        Row[] expected = new Row[]{
        };
        compareRows(expected, cursor(intersectPxPy(31, true, false), queryContext, queryBindings));
        compareRows(expected, cursor(intersectPxPy(31, true, true), queryContext, queryBindings));
        compareRows(expected, cursor(intersectPxPy(32, true, false), queryContext, queryBindings));
        compareRows(expected, cursor(intersectPxPy(32, true, true), queryContext, queryBindings));
        reverse(expected);
        compareRows(expected, cursor(intersectPxPy(31, false, false), queryContext, queryBindings));
        compareRows(expected, cursor(intersectPxPy(31, false, true), queryContext, queryBindings));
        compareRows(expected, cursor(intersectPxPy(32, false, false), queryContext, queryBindings));
        compareRows(expected, cursor(intersectPxPy(32, false, true), queryContext, queryBindings));
    }

    @Test
    public void test4x()
    {
        Row[] expected = new Row[]{
            row(parentXIndexRowType, 44L, 4001L),
            row(parentXIndexRowType, 44L, 4002L),
        };
        // compareRows(expected, cursor(intersectPxPy(44, true, false), context));
        compareRows(expected, cursor(intersectPxPy(44, true, true), queryContext, queryBindings));
        reverse(expected);
        compareRows(expected, cursor(intersectPxPy(44, false, false), queryContext, queryBindings));
        compareRows(expected, cursor(intersectPxPy(44, false, true), queryContext, queryBindings));
    }

    @Test
    public void test5x()
    {
        Row[] expected = new Row[]{
            row(parentXIndexRowType, 55L, 5001L),
            row(parentXIndexRowType, 55L, 5002L),
        };
        compareRows(expected, cursor(intersectPxPy(55, true, false), queryContext, queryBindings));
        compareRows(expected, cursor(intersectPxPy(55, true, true), queryContext, queryBindings));
        reverse(expected);
        compareRows(expected, cursor(intersectPxPy(55, false, false), queryContext, queryBindings));
        compareRows(expected, cursor(intersectPxPy(55, false, true), queryContext, queryBindings));
    }

    @Test
    public void test6x()
    {
        Row[] expected = new Row[]{
            row(parentXIndexRowType, 66L, 6002L),
            row(parentXIndexRowType, 66L, 6003L),
        };
        compareRows(expected, cursor(intersectPxPy(66, true, false), queryContext, queryBindings));
        compareRows(expected, cursor(intersectPxPy(66, true, true), queryContext, queryBindings));
        reverse(expected);
        compareRows(expected, cursor(intersectPxPy(66, false, false), queryContext, queryBindings));
        compareRows(expected, cursor(intersectPxPy(66, false, true), queryContext, queryBindings));
    }

    @Test
    public void test7x()
    {
        Row[] expected = new Row[]{
        };
        compareRows(expected, cursor(intersectPxCz(70, JoinType.INNER_JOIN, true, false), queryContext, queryBindings));
        compareRows(expected, cursor(intersectPxCz(70, JoinType.INNER_JOIN, true, true), queryContext, queryBindings));
        reverse(expected);
        compareRows(expected, cursor(intersectPxCz(70, JoinType.INNER_JOIN, false, false), queryContext, queryBindings));
        compareRows(expected, cursor(intersectPxCz(70, JoinType.INNER_JOIN, false, true), queryContext, queryBindings));
    }

    @Test
    public void test8x()
    {
        Row[] expected = new Row[]{
            row(childRowType, 88L, 8000L, 800000L),
            row(childRowType, 88L, 8001L, 800100L),
            row(childRowType, 88L, 8001L, 800101L),
            row(childRowType, 88L, 8002L, 800200L),
            row(childRowType, 88L, 8002L, 800201L),
            row(childRowType, 88L, 8002L, 800202L),
        };
        compareRows(expected, cursor(intersectPxCz(88, JoinType.INNER_JOIN, true, false), queryContext, queryBindings));
        compareRows(expected, cursor(intersectPxCz(88, JoinType.INNER_JOIN, true, true), queryContext, queryBindings));
        reverse(expected);
        compareRows(expected, cursor(intersectPxCz(88, JoinType.INNER_JOIN, false, false), queryContext, queryBindings));
        compareRows(expected, cursor(intersectPxCz(88, JoinType.INNER_JOIN, false, true), queryContext, queryBindings));
    }

    @Test
    public void test9x()
    {
        Row[] expected = new Row[]{
        };
        compareRows(expected, cursor(intersectPxCz(99, JoinType.INNER_JOIN, true, false), queryContext, queryBindings));
        compareRows(expected, cursor(intersectPxCz(99, JoinType.INNER_JOIN, true, true), queryContext, queryBindings));
        reverse(expected);
        compareRows(expected, cursor(intersectPxCz(99, JoinType.INNER_JOIN, false, false), queryContext, queryBindings));
        compareRows(expected, cursor(intersectPxCz(99, JoinType.INNER_JOIN, false, true), queryContext, queryBindings));
    }

    @Test
    public void test12x()
    {
        Row[] expected = new Row[]{
            row(childRowType, 12L, null, 1200000L),
        };
        compareRows(expected, cursor(intersectPxCz(12, JoinType.RIGHT_JOIN, true, false), queryContext, queryBindings));
        compareRows(expected, cursor(intersectPxCz(12, JoinType.RIGHT_JOIN, true, true), queryContext, queryBindings));
        reverse(expected);
        compareRows(expected, cursor(intersectPxCz(12, JoinType.RIGHT_JOIN, false, false), queryContext, queryBindings));
        compareRows(expected, cursor(intersectPxCz(12, JoinType.RIGHT_JOIN, false, true), queryContext, queryBindings));
    }

    @Test
    public void testNoOrderingFieldsNoComparisonFields()
    {
        Operator plan =
                intersect_Ordered(
                        indexScan_Default(parentPidIndexRowType),
                        indexScan_Default(parentPidIndexRowType),
                        parentPidIndexRowType,
                        parentPidIndexRowType,
                        0,
                        0,
                        0,
                        JoinType.INNER_JOIN,
                        IntersectOption.OUTPUT_LEFT,
                        null, true);
        Row[] expected = new Row[]{
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
            row(parentPidIndexRowType, 9000L),
            row(parentPidIndexRowType, 9002L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    private Operator intersectPxPy(int key, boolean ascending, boolean skipScan)
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
                        EnumSet.of(IntersectOption.OUTPUT_LEFT,
                                skipScan ? IntersectOption.SKIP_SCAN : IntersectOption.SEQUENTIAL_SCAN),
                        null,
                        true);
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
                        EnumSet.of(IntersectOption.OUTPUT_RIGHT,
                                skipScan ? IntersectOption.SKIP_SCAN : IntersectOption.SEQUENTIAL_SCAN),
                        null,
                        true);
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
    
    private boolean[] ascending(boolean ... ascending)
    {
        return ascending;
    }

    private void reverse(Row[] rows)
    {
        int n = rows.length;
        for (int i = 0; i < n / 2; i++) {
            Row r = rows[i];
            rows[i] = rows[n - 1 - i];
            rows[n - 1 - i] = r;
        }
    }
}
