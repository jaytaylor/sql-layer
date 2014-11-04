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

import com.foundationdb.ais.model.Group;
import com.foundationdb.qp.expression.IndexBound;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.server.api.dml.SetColumnSelector;
import org.junit.Test;

import java.util.Collections;

import static com.foundationdb.qp.operator.API.*;

public class BranchLookup_NestedIT extends OperatorITBase
{
    @Override
    protected void setupCreateSchema()
    {
        r = createTable(
            "schema", "r",
            "rid int not null primary key",
            "rvalue varchar(20)");
        createIndex("schema", "r", "rvalue", "rvalue");
        a = createTable(
            "schema", "a",
            "aid int not null primary key",
            "rid int",
            "avalue varchar(20)",
            "grouping foreign key(rid) references r(rid)");
        createIndex("schema", "a", "avalue", "avalue");
        b = createTable(
            "schema", "b",
            "bid int not null primary key",
            "rid int",
            "bvalue varchar(20)",
            "grouping foreign key(rid) references r(rid)");
        createIndex("schema", "b", "bvalue", "bvalue");
        c = createTable(
            "schema", "c",
            "cid int not null primary key",
            "rid int",
            "cvalue varchar(20)",
            "grouping foreign key(rid) references r(rid)");
        createIndex("schema", "c", "cvalue", "cvalue");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        schema = new Schema(ais());
        rRowType = schema.tableRowType(table(r));
        aRowType = schema.tableRowType(table(a));
        bRowType = schema.tableRowType(table(b));
        cRowType = schema.tableRowType(table(c));
        rValueIndexRowType = indexType(r, "rvalue");
        aValueIndexRowType = indexType(a, "avalue");
        bValueIndexRowType = indexType(b, "bvalue");
        cValueIndexRowType = indexType(c, "cvalue");
        rabc = group(r);
        db = new Row[]{   row(r, 1L, "r1"),
                          row(r, 2L, "r2"),
                          row(a, 13L, 1L, "a13"),
                          row(a, 14L, 1L, "a14"),
                          row(a, 23L, 2L, "a23"),
                          row(a, 24L, 2L, "a24"),
                          row(b, 15L, 1L, "b15"),
                          row(b, 16L, 1L, "b16"),
                          row(b, 25L, 2L, "b25"),
                          row(b, 26L, 2L, "b26"),
                          row(c, 17L, 1L, "c17"),
                          row(c, 18L, 1L, "c18"),
                          row(c, 27L, 2L, "c27"),
                          row(c, 28L, 2L, "c28"),
        };
        adapter = newStoreAdapter(schema);
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
        use(db);
    }

    // Test argument validation

    @Test(expected = IllegalArgumentException.class)
    public void testBLNGroupTableNull()
    {
        branchLookup_Nested(null, aRowType, bRowType, InputPreservationOption.KEEP_INPUT, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBLNInputRowTypeNull()
    {
        branchLookup_Nested(rabc, null, bRowType, InputPreservationOption.KEEP_INPUT, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBLNOutputRowTypesEmpty()
    {
        branchLookup_Nested(rabc, aRowType, aRowType, null, Collections.<TableRowType>emptyList(), InputPreservationOption.KEEP_INPUT, 0, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBLNLookupOptionNull()
    {
        branchLookup_Nested(rabc, aRowType, bRowType, null, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBLNBadInputBindingPosition()
    {
        branchLookup_Nested(rabc, aRowType, bRowType, InputPreservationOption.KEEP_INPUT, -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBLNInputTypeNotDescendent()
    {
        branchLookup_Nested(rabc, aRowType, bRowType, bRowType, InputPreservationOption.KEEP_INPUT, -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBLNOutputTypeNotDescendent()
    {
        branchLookup_Nested(rabc, aRowType, aRowType, bRowType, InputPreservationOption.KEEP_INPUT, -1);
    }

    // Test operator execution

    @Test
    public void testAIndexToR()
    {
        Operator plan =
            map_NestedLoops(
                indexScan_Default(aValueIndexRowType),
                branchLookup_Nested(rabc, aValueIndexRowType, rRowType, InputPreservationOption.DISCARD_INPUT, 0),
                0, pipelineMap(), 1);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            // Each r row, and everything below it, is duplicated, because the A index refers to each r value twice.
            row(rRowType, 1L, "r1"),
            row(aRowType, 13L, 1L, "a13"),
            row(aRowType, 14L, 1L, "a14"),
            row(bRowType, 15L, 1L, "b15"),
            row(bRowType, 16L, 1L, "b16"),
            row(cRowType, 17L, 1L, "c17"),
            row(cRowType, 18L, 1L, "c18"),
            row(rRowType, 1L, "r1"),
            row(aRowType, 13L, 1L, "a13"),
            row(aRowType, 14L, 1L, "a14"),
            row(bRowType, 15L, 1L, "b15"),
            row(bRowType, 16L, 1L, "b16"),
            row(cRowType, 17L, 1L, "c17"),
            row(cRowType, 18L, 1L, "c18"),
            row(rRowType, 2L, "r2"),
            row(aRowType, 23L, 2L, "a23"),
            row(aRowType, 24L, 2L, "a24"),
            row(bRowType, 25L, 2L, "b25"),
            row(bRowType, 26L, 2L, "b26"),
            row(cRowType, 27L, 2L, "c27"),
            row(cRowType, 28L, 2L, "c28"),
            row(rRowType, 2L, "r2"),
            row(aRowType, 23L, 2L, "a23"),
            row(aRowType, 24L, 2L, "a24"),
            row(bRowType, 25L, 2L, "b25"),
            row(bRowType, 26L, 2L, "b26"),
            row(cRowType, 27L, 2L, "c27"),
            row(cRowType, 28L, 2L, "c28"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testAToR()
    {
        Operator plan =
            map_NestedLoops(
                ancestorLookup_Default(
                    indexScan_Default(aValueIndexRowType),
                    rabc,
                    aValueIndexRowType,
                    Collections.singleton(aRowType),
                    InputPreservationOption.DISCARD_INPUT),
                branchLookup_Nested(rabc, aRowType, rRowType, InputPreservationOption.DISCARD_INPUT, 0),
                0, pipelineMap(), 1);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            // Each r row, and everything below it, is duplicated, because the A index refers to each r value twice.
            row(rRowType, 1L, "r1"),
            row(aRowType, 13L, 1L, "a13"),
            row(aRowType, 14L, 1L, "a14"),
            row(bRowType, 15L, 1L, "b15"),
            row(bRowType, 16L, 1L, "b16"),
            row(cRowType, 17L, 1L, "c17"),
            row(cRowType, 18L, 1L, "c18"),
            row(rRowType, 1L, "r1"),
            row(aRowType, 13L, 1L, "a13"),
            row(aRowType, 14L, 1L, "a14"),
            row(bRowType, 15L, 1L, "b15"),
            row(bRowType, 16L, 1L, "b16"),
            row(cRowType, 17L, 1L, "c17"),
            row(cRowType, 18L, 1L, "c18"),
            row(rRowType, 2L, "r2"),
            row(aRowType, 23L, 2L, "a23"),
            row(aRowType, 24L, 2L, "a24"),
            row(bRowType, 25L, 2L, "b25"),
            row(bRowType, 26L, 2L, "b26"),
            row(cRowType, 27L, 2L, "c27"),
            row(cRowType, 28L, 2L, "c28"),
            row(rRowType, 2L, "r2"),
            row(aRowType, 23L, 2L, "a23"),
            row(aRowType, 24L, 2L, "a24"),
            row(bRowType, 25L, 2L, "b25"),
            row(bRowType, 26L, 2L, "b26"),
            row(cRowType, 27L, 2L, "c27"),
            row(cRowType, 28L, 2L, "c28"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testAToB()
    {
        Operator plan =
            map_NestedLoops(
                filter_Default(
                    groupScan_Default(rabc),
                    Collections.singleton(aRowType)),
                branchLookup_Nested(rabc, aRowType, bRowType, InputPreservationOption.DISCARD_INPUT, 0),
                0, pipelineMap(), 1);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(bRowType, 15L, 1L, "b15"),
            row(bRowType, 16L, 1L, "b16"),
            row(bRowType, 15L, 1L, "b15"),
            row(bRowType, 16L, 1L, "b16"),
            row(bRowType, 25L, 2L, "b25"),
            row(bRowType, 26L, 2L, "b26"),
            row(bRowType, 25L, 2L, "b25"),
            row(bRowType, 26L, 2L, "b26"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testAToBAndC()
    {
        Operator plan =
            map_NestedLoops(
                map_NestedLoops(
                    filter_Default(
                        groupScan_Default(rabc),
                        Collections.singleton(aRowType)),
                    branchLookup_Nested(rabc, aRowType, bRowType, InputPreservationOption.DISCARD_INPUT, 0),
                    0, pipelineMap(), 1),
                branchLookup_Nested(rabc, bRowType, cRowType, InputPreservationOption.KEEP_INPUT, 1),
                1, pipelineMap(), 1);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(bRowType, 15L, 1L, "b15"),
            row(cRowType, 17L, 1L, "c17"),
            row(cRowType, 18L, 1L, "c18"),
            row(bRowType, 16L, 1L, "b16"),
            row(cRowType, 17L, 1L, "c17"),
            row(cRowType, 18L, 1L, "c18"),
            row(bRowType, 15L, 1L, "b15"),
            row(cRowType, 17L, 1L, "c17"),
            row(cRowType, 18L, 1L, "c18"),
            row(bRowType, 16L, 1L, "b16"),
            row(cRowType, 17L, 1L, "c17"),
            row(cRowType, 18L, 1L, "c18"),
            row(bRowType, 25L, 2L, "b25"),
            row(cRowType, 27L, 2L, "c27"),
            row(cRowType, 28L, 2L, "c28"),
            row(bRowType, 26L, 2L, "b26"),
            row(cRowType, 27L, 2L, "c27"),
            row(cRowType, 28L, 2L, "c28"),
            row(bRowType, 25L, 2L, "b25"),
            row(cRowType, 27L, 2L, "c27"),
            row(cRowType, 28L, 2L, "c28"),
            row(bRowType, 26L, 2L, "b26"),
            row(cRowType, 27L, 2L, "c27"),
            row(cRowType, 28L, 2L, "c28"),
        };
        compareRows(expected, cursor);
    }

    // Multiple index tests

    @Test
    public void testABIndexToC()
    {
        Operator abIndexScan =
            hKeyUnion_Ordered(
                indexScan_Default(aValueIndexRowType, false, aValueRange("a13", "a14")),
                indexScan_Default(bValueIndexRowType, false, bValueRange("b15", "b16")),
                aValueIndexRowType,
                bValueIndexRowType,
                2,
                2,
                2,
                rRowType);
        Operator plan =
            map_NestedLoops(
                abIndexScan,
                branchLookup_Nested(rabc, abIndexScan.rowType(), cRowType, InputPreservationOption.DISCARD_INPUT, 0),
                0, pipelineMap(), 1);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(cRowType, 17L, 1L, "c17"),
            row(cRowType, 18L, 1L, "c18"),
        };
        compareRows(expected, cursor);
    }
    
    @Test
    public void testAToA()
    {
        Operator plan =
                map_NestedLoops(
                        filter_Default(
                                groupScan_Default(rabc),
                                Collections.singleton(aRowType)),
                        branchLookup_Nested(rabc, aRowType, rRowType, aRowType, InputPreservationOption.DISCARD_INPUT, 0),
                        0, pipelineMap(), 1);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
                row(aRowType, 13L, 1L, "a13"),
                row(aRowType, 14L, 1L, "a14"),
                row(aRowType, 13L, 1L, "a13"),
                row(aRowType, 14L, 1L, "a14"),
                row(aRowType, 23L, 2L, "a23"),
                row(aRowType, 24L, 2L, "a24"),
                row(aRowType, 23L, 2L, "a23"),
                row(aRowType, 24L, 2L, "a24"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testCursor()
    {
        Operator plan =
            map_NestedLoops(
                filter_Default(
                    groupScan_Default(rabc),
                    Collections.singleton(aRowType)),
                branchLookup_Nested(rabc, aRowType, rRowType, aRowType, InputPreservationOption.DISCARD_INPUT, 0),
                0, pipelineMap(), 1);
        CursorLifecycleTestCase testCase = new CursorLifecycleTestCase()
        {
            @Override
            public Row[] firstExpectedRows()
            {
                return new Row[] {
                    row(aRowType, 13L, 1L, "a13"),
                    row(aRowType, 14L, 1L, "a14"),
                    row(aRowType, 13L, 1L, "a13"),
                    row(aRowType, 14L, 1L, "a14"),
                    row(aRowType, 23L, 2L, "a23"),
                    row(aRowType, 24L, 2L, "a24"),
                    row(aRowType, 23L, 2L, "a23"),
                    row(aRowType, 24L, 2L, "a24"),
                };
            }

            @Override
            public boolean reopenTopLevel() {
                return true;
            }
        };
        testCursorLifecycle(plan, testCase);
    }

    private IndexKeyRange aValueRange(String lo, String hi)
    {
        return IndexKeyRange.bounded(aValueIndexRowType, aValueBound(lo), true, aValueBound(hi), true);
    }

    private IndexKeyRange bValueRange(String lo, String hi)
    {
        return IndexKeyRange.bounded(bValueIndexRowType, bValueBound(lo), true, bValueBound(hi), true);
    }

    private IndexBound aValueBound(String a)
    {
        return new IndexBound(row(aValueIndexRowType, a), new SetColumnSelector(0));
    }

    private IndexBound bValueBound(String b)
    {
        return new IndexBound(row(bValueIndexRowType, b), new SetColumnSelector(0));
    }

    protected int r;
    protected int a;
    protected int c;
    protected int b;
    protected TableRowType rRowType;
    protected TableRowType aRowType;
    protected TableRowType bRowType;
    protected TableRowType cRowType;
    protected IndexRowType rValueIndexRowType;
    protected IndexRowType aValueIndexRowType;
    protected IndexRowType bValueIndexRowType;
    protected IndexRowType cValueIndexRowType;
    protected Group rabc;
}
