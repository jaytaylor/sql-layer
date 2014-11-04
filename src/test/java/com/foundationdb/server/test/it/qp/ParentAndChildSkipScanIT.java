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

public class ParentAndChildSkipScanIT extends OperatorITBase
{
    @Override
    protected void setupCreateSchema()
    {
        parent = createTable(
            "schema", "parent",
            "pid int not null",
            "x int",
            "primary key(pid)");
        createIndex("schema", "parent", "idx_x", "x");
        child = createTable(
            "schema", "child",
            "cid int not null primary key",
            "pid int",
            "y int",
            "grouping foreign key (pid) references parent(pid)");
        createIndex("schema", "child", "y", "y");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        schema = new Schema(ais());
        parentRowType = schema.tableRowType(table(parent));
        childRowType = schema.tableRowType(table(child));
        parentPidIndexRowType = indexType(parent, "pid");
        parentXIndexRowType = indexType(parent, "x");
        childYIndexRowType = indexType(child, "y");
        adapter = newStoreAdapter(schema);
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
        db = new Row[]{
            row(parent, 60L, 1L),
            row(child, 7000L, 70L, 2L),
            row(parent, 80L, 1L),
            row(child, 8000L, 80L, 2L),
            row(child, 9000L, 90L, 2L),
        };
        use(db);
    }

    private int parent;
    private int child;
    private RowType parentRowType;
    private RowType childRowType;
    private IndexRowType parentPidIndexRowType;
    private IndexRowType parentXIndexRowType;
    private IndexRowType childYIndexRowType;

    @Test
    public void test8x()
    {
        Row[] expected = new Row[] {
            row(childRowType, 2L, 80L, 8000L)
        };
        compareRows(expected, cursor(intersectPxUnionCy(1, 2, 3, JoinType.INNER_JOIN, true, false), queryContext, queryBindings));
        compareRows(expected, cursor(intersectPxUnionCy(1, 2, 3, JoinType.INNER_JOIN, true, true), queryContext, queryBindings));
        compareRows(expected, cursor(intersectPxUnionCy(1, 2, 3, JoinType.INNER_JOIN, false, false), queryContext, queryBindings));
        compareRows(expected, cursor(intersectPxUnionCy(1, 2, 3, JoinType.INNER_JOIN, false, true), queryContext, queryBindings));
    }

    private Operator intersectPxUnionCy(int x, int y1, int y2, JoinType joinType, boolean ascending, boolean skipScan)
    {
        Ordering xOrdering = ordering(field(parentXIndexRowType, 1), ascending);
        Ordering yOrdering = ordering(field(childYIndexRowType, 1), ascending,
                                      field(childYIndexRowType, 2), ascending);
        return
            intersect_Ordered(
                    indexScan_Default(
                            parentXIndexRowType,
                            parentXEq(x),
                            xOrdering),
                    union_Ordered(
                            indexScan_Default(
                                    childYIndexRowType,
                                    childYEq(y1),
                                    yOrdering),
                            indexScan_Default(
                                    childYIndexRowType,
                                    childYEq(y2),
                                    yOrdering),
                            childYIndexRowType,
                            childYIndexRowType,
                            2,
                            2,
                            new boolean[]{ascending, ascending},
                            false),
                    parentXIndexRowType,
                    childYIndexRowType,
                    1,
                    2,
                    ascending(ascending),
                    joinType,
                    EnumSet.of(skipScan ? IntersectOption.SKIP_SCAN : IntersectOption.SEQUENTIAL_SCAN,
                            IntersectOption.OUTPUT_RIGHT),
                    null,
                    true);
    }

    private IndexKeyRange parentXEq(long x)
    {
        IndexBound xBound = new IndexBound(row(parentXIndexRowType, x), new SetColumnSelector(0));
        return IndexKeyRange.bounded(parentXIndexRowType, xBound, true, xBound, true);
    }

    private IndexKeyRange childYEq(long y)
    {
        IndexBound yBound = new IndexBound(row(childYIndexRowType, y), new SetColumnSelector(0));
        return IndexKeyRange.bounded(childYIndexRowType, yBound, true, yBound, true);
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
}
