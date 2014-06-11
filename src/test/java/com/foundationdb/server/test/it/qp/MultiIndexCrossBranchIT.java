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
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.server.api.dml.SetColumnSelector;
import com.foundationdb.server.api.dml.scan.NewRow;
import org.junit.Test;

import static com.foundationdb.qp.operator.API.*;
import static com.foundationdb.qp.operator.API.IntersectOption.*;
import static com.foundationdb.server.test.ExpressionGenerators.field;

public class MultiIndexCrossBranchIT extends OperatorITBase
{
    @Override
    protected void setupCreateSchema()
    {
        p = createTable(
            "schema", "p",
            "pid int not null primary key",
            "x int");
        createIndex("schema", "p", "px", "x");
        c = createTable(
            "schema", "c",
            "cid int not null primary key",
            "pid int",
            "y int",
            "grouping foreign key (pid) references p(pid)");
        createIndex("schema", "c", "cy", "y");
        d = createTable(
            "schema", "d",
            "did int not null primary key",
            "pid int",
            "z int",
            "grouping foreign key (pid) references p(pid)");
        createIndex("schema", "d", "dz", "z");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        schema = new Schema(ais());
        pRowType = schema.tableRowType(table(p));
        cRowType = schema.tableRowType(table(c));
        dRowType = schema.tableRowType(table(d));
        pXIndexRowType = indexType(p, "x");
        cYIndexRowType = indexType(c, "y");
        dZIndexRowType = indexType(d, "z");
        hKeyRowType = schema.newHKeyRowType(pRowType.table().hKey());
        coi = group(p);
        adapter = newStoreAdapter(schema);
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
        db = new NewRow[]{
            // 0x: Both sides empty
            // 1x: C empty
            createNewRow(p, 10L, 1L),
            createNewRow(d, 1900L, 10L, 1L),
            createNewRow(d, 1901L, 10L, 1L),
            createNewRow(d, 1902L, 10L, 1L),
            // 2x: D empty
            createNewRow(p, 20L, 2L),
            createNewRow(c, 2800L, 20L, 2L),
            createNewRow(c, 2801L, 20L, 2L),
            createNewRow(c, 2802L, 20L, 2L),
            // 3x: C, D non-empty
            createNewRow(p, 30L, 3L),
            createNewRow(c, 3800L, 30L, 3L),
            createNewRow(c, 3801L, 30L, 3L),
            createNewRow(c, 3802L, 30L, 3L),
            createNewRow(d, 3900L, 30L, 3L),
            createNewRow(d, 3901L, 30L, 3L),
        };
        use(db);
    }

    @Test
    public void test0xAND()
    {
        Operator plan = intersectCyDz(0, OUTPUT_LEFT);
        Row[] expected = new Row[]{
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
        plan = intersectCyDz(0, OUTPUT_RIGHT);
        expected = new Row[]{
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void test1xAND()
    {
        Operator plan = intersectCyDz(1, OUTPUT_LEFT);
        Row[] expected = new Row[]{
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
        plan = intersectCyDz(1, OUTPUT_RIGHT);
        expected = new Row[]{
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void test2xAND()
    {
        Operator plan = intersectCyDz(2, OUTPUT_LEFT);
        Row[] expected = new Row[]{
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
        plan = intersectCyDz(2, OUTPUT_RIGHT);
        expected = new Row[]{
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void test3xAND()
    {
        Operator plan = intersectCyDz(3, OUTPUT_LEFT);
        Row[] expected = new Row[]{
            row(cRowType, 3L, 30L, 3800L),
            row(cRowType, 3L, 30L, 3801L),
            row(cRowType, 3L, 30L, 3802L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
        plan = intersectCyDz(3, OUTPUT_RIGHT);
        expected = new Row[]{
            row(dRowType, 3L, 30L, 3900L),
            row(dRowType, 3L, 30L, 3901L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void test0xOR()
    {
        Operator plan = unionCyDz(0);
        String[] expected = new String[]{
        };
        compareRenderedHKeys(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void test1xOR()
    {
        Operator plan = unionCyDz(1);
        String[] expected = new String[]{
            pKey(10L),
        };
        compareRenderedHKeys(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void test2xOR()
    {
        Operator plan = unionCyDz(2);
        String[] expected = new String[]{
            pKey(20L),
        };
        compareRenderedHKeys(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void test3xOR()
    {
        Operator plan = unionCyDz(3);
        String[] expected = new String[]{
            pKey(30L),
        };
        compareRenderedHKeys(expected, cursor(plan, queryContext, queryBindings));
    }

    private Operator intersectCyDz(int key, IntersectOption side)
    {
        Operator plan =
            intersect_Ordered(
                    indexScan_Default(
                            cYIndexRowType,
                            cYEQ(key),
                            ordering(field(cYIndexRowType, 1), true,
                                    field(cYIndexRowType, 2), true)),
                    indexScan_Default(
                            dZIndexRowType,
                            dZEQ(key),
                            ordering(field(dZIndexRowType, 1), true,
                                    field(dZIndexRowType, 2), true)),
                    cYIndexRowType,
                    dZIndexRowType,
                    2,
                    2,
                    1,
                    JoinType.INNER_JOIN,
                    side,
                    null,
                    true);
        return plan;
    }

    private Operator unionCyDz(int key)
    {
        Operator plan =
            hKeyUnion_Ordered(
                indexScan_Default(
                    cYIndexRowType,
                    cYEQ(key),
                    ordering(field(cYIndexRowType, 1), true,
                             field(cYIndexRowType, 2), true)),
                indexScan_Default(
                    dZIndexRowType,
                    dZEQ(key),
                    ordering(field(dZIndexRowType, 1), true,
                             field(dZIndexRowType, 2), true)),
                cYIndexRowType,
                dZIndexRowType,
                2,
                2,
                1,
                pRowType);
        return plan;
    }

    private IndexKeyRange cYEQ(long y)
    {
        IndexBound yBound = new IndexBound(row(cYIndexRowType, y), new SetColumnSelector(0));
        return IndexKeyRange.bounded(cYIndexRowType, yBound, true, yBound, true);
    }

    private IndexKeyRange dZEQ(long z)
    {
        IndexBound zBound = new IndexBound(row(dZIndexRowType, z), new SetColumnSelector(0));
        return IndexKeyRange.bounded(dZIndexRowType, zBound, true, zBound, true);
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
        return String.format("{%d,%s}", pRowType.table().getOrdinal(), hKeyValue(pid));
    }

    private int p;
    private int c;
    private int d;
    private TableRowType pRowType;
    private TableRowType cRowType;
    private TableRowType dRowType;
    private IndexRowType pXIndexRowType;
    private IndexRowType cYIndexRowType;
    private IndexRowType dZIndexRowType;
    private RowType hKeyRowType;
}
