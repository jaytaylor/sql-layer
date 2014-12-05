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

import com.foundationdb.qp.expression.ExpressionRow;
import com.foundationdb.qp.expression.IndexBound;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.expression.RowBasedUnboundExpressions;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.ExpressionGenerator;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.BindableRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.api.dml.SetColumnSelector;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.server.types.texpressions.TPreparedLiteral;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import static com.foundationdb.qp.operator.API.*;
import static com.foundationdb.server.test.ExpressionGenerators.boundField;
import static com.foundationdb.server.test.ExpressionGenerators.field;

public class Intersect_OrderedSkipScanIT extends OperatorITBase
{
    @Override
    protected void setupCreateSchema()
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
    }

    @Override
    protected void setupPostCreateSchema()
    {
        parentRowType = schema.tableRowType(table(parent));
        childRowType = schema.tableRowType(table(child));
        parentPidIndexRowType = indexType(parent, "pid");
        parentXIndexRowType = indexType(parent, "x1", "x2");
        parentYIndexRowType = indexType(parent, "y");
        childZIndexRowType = indexType(child, "z");
        coi = group(parent);
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
        db = new Row[]{
            // 0x: Both index scans empty
            // 1x: Left empty
            row(parent, 1000L, -1L, -1L, 11L),
            row(parent, 1001L, -1L, -1L, 11L),
            row(parent, 1002L, -1L, -1L, 11L),
            // 2x: Right empty
            row(parent, 2000L, 22L, 22L, -1L),
            row(parent, 2001L, 22L, 22L, -1L),
            row(parent, 2002L, 22L, 22L, -1L),
            // 3x: Both non-empty, and no overlap
            row(parent, 3000L, 31L, 31L, -1L),
            row(parent, 3001L, 31L, 31L, -1L),
            row(parent, 3002L, 31L, 31L, -1L),
            row(parent, 3003L, 9999L, 9999L, 32L),
            row(parent, 3004L, 9999L, 9999L, 32L),
            row(parent, 3005L, 9999L, 9999L, 32L),
            // 4x: left contains right
            row(parent, 4000L, 44L, 44L, -1L),
            row(parent, 4001L, 44L, 44L, 44L),
            row(parent, 4002L, 44L, 44L, 44L),
            row(parent, 4003L, 44L, 44L, 9999L),
            // 5x: right contains left
            row(parent, 5000L, -1L, -1L, 55L),
            row(parent, 5001L, 55L, 55L, 55L),
            row(parent, 5002L, 55L, 55L, 55L),
            row(parent, 5003L, 9999L, 9999L, 55L),
            // 6x: overlap but neither side contains the other
            row(parent, 6000L, -1L, -1L, 66L),
            row(parent, 6001L, -1L, -1L, 66L),
            row(parent, 6002L, 66L, 66L, 66L),
            row(parent, 6003L, 66L, 66L, 66L),
            row(parent, 6004L, 66L, 66L, 9999L),
            row(parent, 6005L, 66L, 66L, 9999L),
            // 7x: parent with no children
            row(parent, 7000L, 70L, 70L, 70L),
            // 8x: parent with children
            row(parent, 8000L, 88L, 88L, 88L),
            row(child, 800000L, 8000L, 88L),
            row(parent, 8001L, 88L, 88L, 88L),
            row(child, 800100L, 8001L, 88L),
            row(child, 800101L, 8001L, 88L),
            row(parent, 8002L, 88L, 88L, 88L),
            row(child, 800200L, 8002L, 88L),
            row(child, 800201L, 8002L, 88L),
            row(child, 800202L, 8002L, 88L),
            // 9x child with no parent
            row(child, 900000L, 9000L, 99L),
            // 12x right join (child on right)
            row(child, 1200000L, null, 12L),
            // 13x skip in both directions
            row(parent, 13000L, -1L, -1L, 13L),
            row(parent, 13001L, -1L, -1L, 13L),
            row(parent, 13002L, 13L, 13L, 13L),
            row(parent, 13003L, 13L, 13L, -1L),
            row(parent, 13004L, 13L, 13L, -1L),
            row(parent, 13005L, 13L, 13L, 13L),
            row(parent, 13006L, -1L, -1L, 13L),
            row(parent, 13007L, -1L, -1L, 13L),
            row(parent, 13008L, -1L, -1L, 13L),
            row(parent, 13009L, -1L, -1L, 13L),
            row(parent, 13010L, 13L, 13L, 13L),
            row(parent, 13011L, 13L, 13L, 13L),
            row(parent, 13012L, 13L, 13L, 13L),
            row(parent, 13013L, 13L, 13L, -1L),
            row(parent, 13014L, 13L, 13L, -1L),
            row(parent, 13015L, 13L, 13L, -1L),
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
        Row[] expectedX = new Row[]{
        };
        compareRows(expectedX, cursor(intersectPxPy(0, true, true, false), queryContext, queryBindings));
        compareRows(expectedX, cursor(intersectPxPy(0, true, true, true), queryContext, queryBindings));
        reverse(expectedX);
        compareRows(expectedX, cursor(intersectPxPy(0, true, false, false), queryContext, queryBindings));
        compareRows(expectedX, cursor(intersectPxPy(0, true, false, true), queryContext, queryBindings));
        Row[] expectedY = new Row[]{
        };
        compareRows(expectedY, cursor(intersectPxPy(0, false, true, false), queryContext, queryBindings));
        compareRows(expectedY, cursor(intersectPxPy(0, false, true, true), queryContext, queryBindings));
        reverse(expectedY);
        compareRows(expectedY, cursor(intersectPxPy(0, false, false, false), queryContext, queryBindings));
        compareRows(expectedY, cursor(intersectPxPy(0, false, false, true), queryContext, queryBindings));
    }

    @Test
    public void test1x()
    {
        Row[] expectedX = new Row[]{
        };
        compareRows(expectedX, cursor(intersectPxPy(11, true, true, false), queryContext, queryBindings));
        compareRows(expectedX, cursor(intersectPxPy(11, true, true, true), queryContext, queryBindings));
        reverse(expectedX);
        compareRows(expectedX, cursor(intersectPxPy(11, true, false, false), queryContext, queryBindings));
        compareRows(expectedX, cursor(intersectPxPy(11, true, false, true), queryContext, queryBindings));
        Row[] expectedY = new Row[]{
        };
        compareRows(expectedY, cursor(intersectPxPy(11, false, true, false), queryContext, queryBindings));
        compareRows(expectedY, cursor(intersectPxPy(11, false, true, true), queryContext, queryBindings));
        reverse(expectedY);
        compareRows(expectedY, cursor(intersectPxPy(11, false, false, false), queryContext, queryBindings));
        compareRows(expectedY, cursor(intersectPxPy(11, false, false, true), queryContext, queryBindings));
    }

    @Test
    public void test2x()
    {
        Row[] expectedX = new Row[]{
        };
        compareRows(expectedX, cursor(intersectPxPy(22, true, true, false), queryContext, queryBindings));
        compareRows(expectedX, cursor(intersectPxPy(22, true, true, true), queryContext, queryBindings));
        reverse(expectedX);
        compareRows(expectedX, cursor(intersectPxPy(22, true, false, false), queryContext, queryBindings));
        compareRows(expectedX, cursor(intersectPxPy(22, true, false, true), queryContext, queryBindings));
        Row[] expectedY = new Row[]{
        };
        compareRows(expectedY, cursor(intersectPxPy(22, false, true, false), queryContext, queryBindings));
        compareRows(expectedY, cursor(intersectPxPy(22, false, true, true), queryContext, queryBindings));
        reverse(expectedY);
        compareRows(expectedY, cursor(intersectPxPy(22, false, false, false), queryContext, queryBindings));
        compareRows(expectedY, cursor(intersectPxPy(22, false, false, true), queryContext, queryBindings));
    }

    @Test
    public void test3x()
    {
        Row[] expectedX = new Row[]{
        };
        compareRows(expectedX, cursor(intersectPxPy(31, true, true, false), queryContext, queryBindings));
        compareRows(expectedX, cursor(intersectPxPy(31, true, true, true), queryContext, queryBindings));
        compareRows(expectedX, cursor(intersectPxPy(32, true, true, false), queryContext, queryBindings));
        compareRows(expectedX, cursor(intersectPxPy(32, true, true, true), queryContext, queryBindings));
        reverse(expectedX);
        compareRows(expectedX, cursor(intersectPxPy(31, true, false, false), queryContext, queryBindings));
        compareRows(expectedX, cursor(intersectPxPy(31, true, false, true), queryContext, queryBindings));
        compareRows(expectedX, cursor(intersectPxPy(32, true, false, false), queryContext, queryBindings));
        compareRows(expectedX, cursor(intersectPxPy(32, true, false, true), queryContext, queryBindings));
        Row[] expectedY = new Row[]{
        };
        compareRows(expectedY, cursor(intersectPxPy(31, false, true, false), queryContext, queryBindings));
        compareRows(expectedY, cursor(intersectPxPy(31, false, true, true), queryContext, queryBindings));
        compareRows(expectedY, cursor(intersectPxPy(32, false, true, false), queryContext, queryBindings));
        compareRows(expectedY, cursor(intersectPxPy(32, false, true, true), queryContext, queryBindings));
        reverse(expectedY);
        compareRows(expectedY, cursor(intersectPxPy(31, false, false, false), queryContext, queryBindings));
        compareRows(expectedY, cursor(intersectPxPy(31, false, false, true), queryContext, queryBindings));
        compareRows(expectedY, cursor(intersectPxPy(32, false, false, false), queryContext, queryBindings));
        compareRows(expectedY, cursor(intersectPxPy(32, false, false, true), queryContext, queryBindings));
    }

    @Test
    public void test4x()
    {
        Row[] expectedX = new Row[]{
            row(parentXIndexRowType, 44L, 44L, 4001L),
            row(parentXIndexRowType, 44L, 44L, 4002L),
        };
        compareRows(expectedX, cursor(intersectPxPy(44, true, true, false), queryContext, queryBindings));
        compareRows(expectedX, cursor(intersectPxPy(44, true, true, true), queryContext, queryBindings));
        reverse(expectedX);
        compareRows(expectedX, cursor(intersectPxPy(44, true, false, false), queryContext, queryBindings));
        compareRows(expectedX, cursor(intersectPxPy(44, true, false, true), queryContext, queryBindings));
        Row[] expectedY = new Row[]{
            row(parentYIndexRowType, 44L, 4001L),
            row(parentYIndexRowType, 44L, 4002L),
        };
        compareRows(expectedY, cursor(intersectPxPy(44, false, true, false), queryContext, queryBindings));
        compareRows(expectedY, cursor(intersectPxPy(44, false, true, true), queryContext, queryBindings));
        reverse(expectedY);
        compareRows(expectedY, cursor(intersectPxPy(44, false, false, false), queryContext, queryBindings));
        compareRows(expectedY, cursor(intersectPxPy(44, false, false, true), queryContext, queryBindings));
    }

    @Test
    public void test5x()
    {
        Row[] expectedX = new Row[]{
            row(parentXIndexRowType, 55L, 55L, 5001L),
            row(parentXIndexRowType, 55L, 55L, 5002L),
        };
        compareRows(expectedX, cursor(intersectPxPy(55, true, true, false), queryContext, queryBindings));
        compareRows(expectedX, cursor(intersectPxPy(55, true, true, true), queryContext, queryBindings));
        reverse(expectedX);
        compareRows(expectedX, cursor(intersectPxPy(55, true, false, false), queryContext, queryBindings));
        compareRows(expectedX, cursor(intersectPxPy(55, true, false, true), queryContext, queryBindings));
        Row[] expectedY = new Row[]{
            row(parentYIndexRowType, 55L, 5001L),
            row(parentYIndexRowType, 55L, 5002L),
        };
        compareRows(expectedY, cursor(intersectPxPy(55, false, true, false), queryContext, queryBindings));
        compareRows(expectedY, cursor(intersectPxPy(55, false, true, true), queryContext, queryBindings));
        reverse(expectedY);
        compareRows(expectedY, cursor(intersectPxPy(55, false, false, false), queryContext, queryBindings));
        compareRows(expectedY, cursor(intersectPxPy(55, false, false, true), queryContext, queryBindings));
    }

    @Test
    public void test6x()
    {
        Row[] expectedX = new Row[]{
            row(parentXIndexRowType, 66L, 66L, 6002L),
            row(parentXIndexRowType, 66L, 66L, 6003L),
        };
        compareRows(expectedX, cursor(intersectPxPy(66, true, true, false), queryContext, queryBindings));
        compareRows(expectedX, cursor(intersectPxPy(66, true, true, true), queryContext, queryBindings));
        reverse(expectedX);
        compareRows(expectedX, cursor(intersectPxPy(66, true, false, false), queryContext, queryBindings));
        compareRows(expectedX, cursor(intersectPxPy(66, true, false, true), queryContext, queryBindings));
        Row[] expectedY = new Row[]{
            row(parentYIndexRowType, 66L, 6002L),
            row(parentYIndexRowType, 66L, 6003L),
        };
        compareRows(expectedY, cursor(intersectPxPy(66, false, true, false), queryContext, queryBindings));
        compareRows(expectedY, cursor(intersectPxPy(66, false, true, true), queryContext, queryBindings));
        reverse(expectedY);
        compareRows(expectedY, cursor(intersectPxPy(66, false, false, false), queryContext, queryBindings));
        compareRows(expectedY, cursor(intersectPxPy(66, false, false, true), queryContext, queryBindings));
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
    public void test13x()
    {
        Row[] expectedX = new Row[]{
            row(parentXIndexRowType, 13L, 13L, 13002L),
            row(parentXIndexRowType, 13L, 13L, 13005L),
            row(parentXIndexRowType, 13L, 13L, 13010L),
            row(parentXIndexRowType, 13L, 13L, 13011L),
            row(parentXIndexRowType, 13L, 13L, 13012L),
        };
        compareRows(expectedX, cursor(intersectPxPy(13, true, true, false), queryContext, queryBindings));
        compareRows(expectedX, cursor(intersectPxPy(13, true, true, true), queryContext, queryBindings));
        reverse(expectedX);
        compareRows(expectedX, cursor(intersectPxPy(13, true, false, false), queryContext, queryBindings));
        compareRows(expectedX, cursor(intersectPxPy(13, true, false, true), queryContext, queryBindings));
        Row[] expectedY = new Row[]{
            row(parentYIndexRowType, 13L, 13002L),
            row(parentYIndexRowType, 13L, 13005L),
            row(parentYIndexRowType, 13L, 13010L),
            row(parentYIndexRowType, 13L, 13011L),
            row(parentYIndexRowType, 13L, 13012L),
        };
        compareRows(expectedY, cursor(intersectPxPy(13, false, true, false), queryContext, queryBindings));
        compareRows(expectedY, cursor(intersectPxPy(13, false, true, true), queryContext, queryBindings));
        reverse(expectedY);
        compareRows(expectedY, cursor(intersectPxPy(13, false, false, false), queryContext, queryBindings));
        compareRows(expectedY, cursor(intersectPxPy(13, false, false, true), queryContext, queryBindings));
    }

    @Test
    public void test4x5x()
    {
        int[] keys = { 44, 55 };
        Row[] expectedX = new Row[]{
            row(parentXIndexRowType, 44L, 44L, 4001L),
            row(parentXIndexRowType, 44L, 44L, 4002L),
            row(parentXIndexRowType, 55L, 55L, 5001L),
            row(parentXIndexRowType, 55L, 55L, 5002L),
        };
        compareRows(expectedX, cursor(intersectPxPyMap(keys, true, true, false), queryContext, queryBindings));
        compareRows(expectedX, cursor(intersectPxPyMap(keys, true, true, true), queryContext, queryBindings));
        Row[] expectedY = new Row[]{
            row(parentYIndexRowType, 44L, 4001L),
            row(parentYIndexRowType, 44L, 4002L),
            row(parentYIndexRowType, 55L, 5001L),
            row(parentYIndexRowType, 55L, 5002L),
        };
        compareRows(expectedY, cursor(intersectPxPyMap(keys, false, true, false), queryContext, queryBindings));
        compareRows(expectedY, cursor(intersectPxPyMap(keys, false, true, true), queryContext, queryBindings));
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
                                    : IntersectOption.OUTPUT_RIGHT),
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
                    EnumSet.of(skipScan
                                    ? IntersectOption.SKIP_SCAN
                                    : IntersectOption.SEQUENTIAL_SCAN,
                            IntersectOption.OUTPUT_RIGHT),
                    null,
                    true);
        return plan;
    }

    private Operator intersectPxPyMap(int[] keys, boolean leftOutput, boolean ascending, boolean skipScan)
    {
        TInstance intType = MNumeric.INT.instance(false);
        RowType xyValueRowType = schema.newValuesType(intType);
        List<BindableRow> keyRows = new ArrayList<>(keys.length);
        for (int key : keys) {
            List<TPreparedExpression> pExpressions =
                Arrays.<TPreparedExpression>asList(new TPreparedLiteral(intType, new Value(intType, key)));
            Row row = new ExpressionRow(xyValueRowType, queryContext, queryBindings, pExpressions);
            keyRows.add(BindableRow.of(row));
        }
        List<ExpressionGenerator> expressions = Arrays.asList(boundField(xyValueRowType, 0, 0));
        IndexBound xBound =
            new IndexBound(
                new RowBasedUnboundExpressions(parentXIndexRowType, expressions, true),
                new SetColumnSelector(0));
        IndexKeyRange xRange = IndexKeyRange.bounded(parentXIndexRowType, xBound, true, xBound, true);
        IndexBound yBound =
            new IndexBound(
                new RowBasedUnboundExpressions(parentYIndexRowType, expressions, true),
                new SetColumnSelector(0));
        IndexKeyRange yRange = IndexKeyRange.bounded(parentYIndexRowType, yBound, true, yBound, true);
        Operator plan =
            map_NestedLoops(
                valuesScan_Default(keyRows, xyValueRowType),
                intersect_Ordered(
                        indexScan_Default(
                                parentXIndexRowType,
                                xRange,
                                ordering(field(parentXIndexRowType, 1), ascending)),
                        indexScan_Default(
                                parentYIndexRowType,
                                yRange,
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
                                        : IntersectOption.OUTPUT_RIGHT),
                        null,
                        true),
                0, pipelineMap(), 1);
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
