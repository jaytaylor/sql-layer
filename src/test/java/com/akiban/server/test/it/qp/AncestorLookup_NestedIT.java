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

import com.akiban.ais.model.GroupTable;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.api.dml.scan.NewRow;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static com.akiban.qp.operator.API.*;

public class AncestorLookup_NestedIT extends OperatorITBase
{
    @Before
    public void before()
    {
        // Don't call super.before(). This is a different schema from most operator ITs.
        r = createTable(
            "schema", "r",
            "rid int not null key",
            "rvalue varchar(20)," +
            "index(rvalue)");
        a = createTable(
            "schema", "a",
            "aid int not null key",
            "rid int",
            "avalue varchar(20)",
            "constraint __akiban_ra foreign key __akiban_ra(rid) references r(rid)",
            "index(avalue)");
        b = createTable(
            "schema", "b",
            "bid int not null key",
            "rid int",
            "bvalue varchar(20)",
            "constraint __akiban_rb foreign key __akiban_rb(rid) references r(rid)",
            "index(bvalue)");
        c = createTable(
            "schema", "c",
            "cid int not null key",
            "rid int",
            "cvalue varchar(20)",
            "constraint __akiban_rc foreign key __akiban_rc(rid) references r(rid)",
            "index(cvalue)");
        schema = new Schema(rowDefCache().ais());
        rRowType = schema.userTableRowType(userTable(r));
        aRowType = schema.userTableRowType(userTable(a));
        bRowType = schema.userTableRowType(userTable(b));
        cRowType = schema.userTableRowType(userTable(c));
        aValueIndexRowType = indexType(a, "avalue");
        rValueIndexRowType = indexType(r, "rvalue");
        rabc = groupTable(r);
        db = new NewRow[]{createNewRow(r, 1L, "r1"),
                          createNewRow(r, 2L, "r2"),
                          createNewRow(a, 13L, 1L, "a13"),
                          createNewRow(a, 14L, 1L, "a14"),
                          createNewRow(a, 23L, 2L, "a23"),
                          createNewRow(a, 24L, 2L, "a24"),
                          createNewRow(b, 15L, 1L, "b15"),
                          createNewRow(b, 16L, 1L, "b16"),
                          createNewRow(b, 25L, 2L, "b25"),
                          createNewRow(b, 26L, 2L, "b26"),
                          createNewRow(c, 17L, 1L, "c17"),
                          createNewRow(c, 18L, 1L, "c18"),
                          createNewRow(c, 27L, 2L, "c27"),
                          createNewRow(c, 28L, 2L, "c28"),
        };
        adapter = persistitAdapter(schema);
        use(db);
    }

    // Test argument validation

    @Test(expected = IllegalArgumentException.class)
    public void testALNGroupTableNull()
    {
        ancestorLookup_Nested(null, aValueIndexRowType, Collections.singleton(aRowType), 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testALNRowTypeNull()
    {
        ancestorLookup_Nested(rabc, null, Collections.singleton(aRowType), 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testALNAncestorTypesNull()
    {
        ancestorLookup_Nested(rabc, aValueIndexRowType, null, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testALNAncestorTypesEmpty()
    {
        ancestorLookup_Nested(rabc, aValueIndexRowType, Collections.<RowType>emptyList(), 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testALNBadBindingPosition()
    {
        ancestorLookup_Nested(rabc, aValueIndexRowType, Collections.singleton(aRowType), -1);
    }

    // Test operator execution

    @Test
    public void testAIndexToA()
    {
        Operator plan =
            map_NestedLoops(
                indexScan_Default(aValueIndexRowType),
                ancestorLookup_Nested(rabc, aValueIndexRowType, Collections.singleton(aRowType), 0),
                0);
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{
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
                ancestorLookup_Nested(rabc, aValueIndexRowType, Arrays.asList(aRowType, rRowType), 0),
                0);
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{
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
                ancestorLookup_Nested(rabc, aValueIndexRowType, Arrays.asList(rRowType, aRowType), 0),
                0);
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{
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
                    ancestorLookup_Nested(rabc, aValueIndexRowType, Arrays.asList(aRowType), 0),
                    0),
                ancestorLookup_Nested(rabc, aRowType, Arrays.asList(rRowType), 1),
                1);
        Cursor cursor = cursor(plan, adapter);
        RowBase[] expected = new RowBase[]{
            row(rRowType, 1L, "r1"),
            row(rRowType, 1L, "r1"),
            row(rRowType, 2L, "r2"),
            row(rRowType, 2L, "r2"),
        };
        compareRows(expected, cursor);
    }

    protected int r;
    protected int a;
    protected int c;
    protected int b;
    protected RowType rRowType;
    protected RowType aRowType;
    protected RowType cRowType;
    protected RowType bRowType;
    protected IndexRowType aValueIndexRowType;
    protected IndexRowType rValueIndexRowType;
    protected GroupTable rabc;
}
