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

import static com.foundationdb.qp.operator.API.cursor;
import static com.foundationdb.qp.operator.API.groupScan_Default;
import static com.foundationdb.qp.operator.API.select_HKeyOrdered;
import static com.foundationdb.qp.operator.API.unionAll_Default;
import static com.foundationdb.qp.operator.API.union_Ordered;

import org.junit.Test;

import com.foundationdb.ais.model.Group;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.error.SetWrongTypeColumns;
import com.foundationdb.server.test.ExpressionGenerators;
import com.foundationdb.server.types.texpressions.Comparison;

public class UnionAllIT extends OperatorITBase {

    @Override
    protected void setupCreateSchema()
    {
        t = createTable(
            "schema", "t",
            "id int not null",
            "x int",
            "primary key(id)");
        createIndex("schema", "t", "tx", "x");
        u = createTable (
                "schema", "u",
                "id int not null primary key",
                "x int");
        v = createTable (
             "schema", "v",
             "id int not null primary key",
             "name varchar(32)");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        schema = new Schema(ais());
        tRowType = schema.tableRowType(table(t));
        uRowType = schema.tableRowType(table(u));
        vRowType = schema.tableRowType(table(v));
        tGroupTable = group(t);
        uGroupTable = group(u);
        vGroupTable = group(v);
        adapter = newStoreAdapter(schema);
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
        db = new NewRow[]{
            createNewRow(t, 1000L, 8L),
            createNewRow(t, 1001L, 9L),
            createNewRow(t, 1002L, 8L),
            createNewRow(t, 1003L, 9L),
            createNewRow(t, 1004L, 8L),
            createNewRow(t, 1005L, 9L),
            createNewRow(t, 1006L, 8L),
            createNewRow(t, 1007L, 9L),
            createNewRow(u, 1000L, 7L),
            createNewRow(u, 1001L, 9L),
            createNewRow(u, 1002L, 9L),
            createNewRow(u, 1003L, 9L)
        };
        use(db);
    }
    private int t, u, v;
    private TableRowType tRowType, uRowType, vRowType;
    private Group tGroupTable, uGroupTable, vGroupTable;
    
    @Test
    public void testBothNonEmpty()
    {
        Operator plan =
            unionAll_Default(
                select_HKeyOrdered(
                    groupScan_Default(tGroupTable),
                    tRowType,
                    ExpressionGenerators.compare(
                        ExpressionGenerators.field(tRowType, 1),
                        Comparison.EQ,
                        ExpressionGenerators.literal(8), castResolver())),
                tRowType,
                select_HKeyOrdered(
                    groupScan_Default(uGroupTable),
                    uRowType,
                    ExpressionGenerators.compare(
                        ExpressionGenerators.field(uRowType, 1),
                        Comparison.EQ,
                        ExpressionGenerators.literal(7), castResolver())),
                uRowType, 
                true);
        TestRow[] expected = new TestRow[]{
            row(tRowType, 1000L, 8L),
            row(tRowType, 1002L, 8L),
            row(tRowType, 1004L, 8L),
            row(tRowType, 1006L, 8L),
            row(uRowType, 1000L, 7L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }
    
    @Test
    public void testOrderedNonEmpty()
    {
        Operator plan =
            union_Ordered(
                select_HKeyOrdered(
                    groupScan_Default(tGroupTable),
                    tRowType,
                    ExpressionGenerators.compare(
                        ExpressionGenerators.field(tRowType, 1),
                        Comparison.EQ,
                        ExpressionGenerators.literal(9), castResolver())),
                select_HKeyOrdered(
                    groupScan_Default(uGroupTable),
                    uRowType,
                    ExpressionGenerators.compare(
                        ExpressionGenerators.field(uRowType, 1),
                        Comparison.EQ,
                        ExpressionGenerators.literal(9), castResolver())),
                tRowType,
                uRowType, 
                2,
                2,
                ascending (true, true),
                false);
        TestRow[] expected = new TestRow[]{
            row(tRowType, 1001L, 9L),
            row(uRowType, 1002L, 9L),
            row(tRowType, 1003L, 9L),
            row(tRowType, 1005L, 9L),
            row(tRowType, 1007L, 9L)
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }
    
    @Test(expected = SetWrongTypeColumns.class)
    public void testAllRowTypeMismatch () {
        unionAll_Default (
                groupScan_Default(tGroupTable),
                tRowType,
                groupScan_Default(vGroupTable),
                vRowType,
                true);
    }
    
    @Test (expected = SetWrongTypeColumns.class)
    public void testDifferentInputTypes() 
    {
        // Test different input types
        union_Ordered(groupScan_Default(tGroupTable),
                      groupScan_Default(vGroupTable),
                      tRowType,
                      vRowType,
                      2,
                      2,
                      ascending(true,true),
                      false);
    }

    private boolean[] ascending(boolean... ascending)
    {
        return ascending;
    }
    
}
