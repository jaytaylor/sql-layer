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

import java.util.Arrays;
import java.util.Collections;

import static com.foundationdb.qp.operator.API.*;

public class AncestorLookup_NestedIT extends OperatorITBase
{
    @Override
    protected void setupCreateSchema() {
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
            "grouping foreign key (rid) references r(rid)");
        createIndex("schema", "a", "avalue", "avalue");
        b = createTable(
            "schema", "b",
            "bid int not null primary key",
            "rid int",
            "bvalue varchar(20)",
            "grouping foreign key (rid) references r(rid)");
        createIndex("schema", "b", "bvalue", "bvalue");
        c = createTable(
            "schema", "c",
            "cid int not null primary key",
            "rid int",
            "cvalue varchar(20)",
            "grouping foreign key (rid) references r(rid)");
        createIndex("schema", "c", "cvalue", "cvalue");
    }

    @Override
    protected void setupPostCreateSchema() {
        schema = new Schema(ais());
        rRowType = schema.tableRowType(table(r));
        aRowType = schema.tableRowType(table(a));
        bRowType = schema.tableRowType(table(b));
        cRowType = schema.tableRowType(table(c));
        aValueIndexRowType = indexType(a, "avalue");
        bValueIndexRowType = indexType(b, "bvalue");
        cValueIndexRowType = indexType(c, "cvalue");
        rValueIndexRowType = indexType(r, "rvalue");
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

    protected int lookaheadQuantum() {
        return 1;
    }

    // Test argument validation

    @Test(expected = IllegalArgumentException.class)
    public void testALNGroupTableNull()
    {
        ancestorLookup_Nested(null, aValueIndexRowType, Collections.singleton(aRowType), 0, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testALNRowTypeNull()
    {
        ancestorLookup_Nested(rabc, null, Collections.singleton(aRowType), 0, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testALNAncestorTypesNull()
    {
        ancestorLookup_Nested(rabc, aValueIndexRowType, null, 0, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testALNAncestorTypesEmpty()
    {
        ancestorLookup_Nested(rabc, aValueIndexRowType, Collections.<TableRowType>emptyList(), 0, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testALNBadBindingPosition()
    {
        ancestorLookup_Nested(rabc, aValueIndexRowType, Collections.singleton(aRowType), -1, 1);
    }

    // Test operator execution

    @Test
    public void testAIndexToA()
    {
        Operator plan =
            map_NestedLoops(
                indexScan_Default(aValueIndexRowType),
                ancestorLookup_Nested(rabc, aValueIndexRowType, Collections.singleton(aRowType), 0, lookaheadQuantum()),
                0, pipelineMap(), 1);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(aRowType, 13L, 1L, "a13"),
            row(aRowType, 14L, 1L, "a14"),
            row(aRowType, 23L, 2L, "a23"),
            row(aRowType, 24L, 2L, "a24"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testAIndexToAAndR()
    {
        Operator plan =
            map_NestedLoops(
                indexScan_Default(aValueIndexRowType),
                ancestorLookup_Nested(rabc, aValueIndexRowType, Arrays.asList(aRowType, rRowType), 0, lookaheadQuantum()),
                0, pipelineMap(), 1);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(rRowType, 1L, "r1"),
            row(aRowType, 13L, 1L, "a13"),
            row(rRowType, 1L, "r1"),
            row(aRowType, 14L, 1L, "a14"),
            row(rRowType, 2L, "r2"),
            row(aRowType, 23L, 2L, "a23"),
            row(rRowType, 2L, "r2"),
            row(aRowType, 24L, 2L, "a24"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testAIndexToARAndA()
    {
        Operator plan =
            map_NestedLoops(
                indexScan_Default(aValueIndexRowType),
                ancestorLookup_Nested(rabc, aValueIndexRowType, Arrays.asList(rRowType, aRowType), 0, lookaheadQuantum()),
                0, pipelineMap(), 1);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(rRowType, 1L, "r1"),
            row(aRowType, 13L, 1L, "a13"),
            row(rRowType, 1L, "r1"),
            row(aRowType, 14L, 1L, "a14"),
            row(rRowType, 2L, "r2"),
            row(aRowType, 23L, 2L, "a23"),
            row(rRowType, 2L, "r2"),
            row(aRowType, 24L, 2L, "a24"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testAIndexToAToR()
    {
        Operator plan =
            map_NestedLoops(
                map_NestedLoops(
                    indexScan_Default(aValueIndexRowType),
                    ancestorLookup_Nested(rabc, aValueIndexRowType, Arrays.asList(aRowType), 0, lookaheadQuantum()),
                    0, pipelineMap(), 1),
                ancestorLookup_Nested(rabc, aRowType, Arrays.asList(rRowType), 1, lookaheadQuantum()),
                1, pipelineMap(), 1);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(rRowType, 1L, "r1"),
            row(rRowType, 1L, "r1"),
            row(rRowType, 2L, "r2"),
            row(rRowType, 2L, "r2"),
        };
        compareRows(expected, cursor);
    }
    
    // Multiple index tests
    
    @Test
    public void testABIndexToR()
    {
        Operator abIndexScan = 
            hKeyUnion_Ordered(
                indexScan_Default(aValueIndexRowType, false, aValueRange("a13", "a14")),
                indexScan_Default(bValueIndexRowType, false, bValueRange("b25", "b26")),
                aValueIndexRowType,
                bValueIndexRowType,
                2,
                2,
                2,
                rRowType);
        Operator plan =
            map_NestedLoops(
                abIndexScan,
                ancestorLookup_Nested(rabc, abIndexScan.rowType(), Collections.singleton(rRowType), 0, lookaheadQuantum()),
                0, pipelineMap(), 1);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(rRowType, 1L, "r1"),
            row(rRowType, 2L, "r2"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testABCIndexToR()
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
        Operator abcIndexScan =
            hKeyUnion_Ordered(
                abIndexScan,
                indexScan_Default(cValueIndexRowType, false, cValueRange("c17", "c18")),
                abIndexScan.rowType(),
                cValueIndexRowType,
                1,
                2,
                1,
                rRowType);
        Operator plan =
            map_NestedLoops(
                abcIndexScan,
                ancestorLookup_Nested(rabc, abcIndexScan.rowType(), Collections.singleton(rRowType), 0, lookaheadQuantum()),
                0, pipelineMap(), 1);
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        Row[] expected = new Row[]{
            row(rRowType, 1L, "r1"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testCursor()
    {
        Operator plan =
            map_NestedLoops(
                indexScan_Default(aValueIndexRowType),
                ancestorLookup_Nested(rabc, aValueIndexRowType, Collections.singleton(aRowType), 0, lookaheadQuantum()),
                0, pipelineMap(), 1);
        CursorLifecycleTestCase testCase = new CursorLifecycleTestCase()
        {
            @Override
            public Row[] firstExpectedRows()
            {
                return new Row[] {
                    row(aRowType, 13L, 1L, "a13"),
                    row(aRowType, 14L, 1L, "a14"),
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

    private IndexKeyRange cValueRange(String lo, String hi)
    {
        return IndexKeyRange.bounded(cValueIndexRowType, cValueBound(lo), true, cValueBound(hi), true);
    }

    private IndexBound aValueBound(String a)
    {
        return new IndexBound(row(aValueIndexRowType, a), new SetColumnSelector(0));
    }

    private IndexBound bValueBound(String b)
    {
        return new IndexBound(row(bValueIndexRowType, b), new SetColumnSelector(0));
    }

    private IndexBound cValueBound(String c)
    {
        return new IndexBound(row(cValueIndexRowType, c), new SetColumnSelector(0));
    }

    protected int r;
    protected int a;
    protected int c;
    protected int b;
    protected TableRowType rRowType;
    protected TableRowType aRowType;
    protected TableRowType cRowType;
    protected TableRowType bRowType;
    protected IndexRowType aValueIndexRowType;
    protected IndexRowType bValueIndexRowType;
    protected IndexRowType cValueIndexRowType;
    protected IndexRowType rValueIndexRowType;
    protected Group rabc;
}
