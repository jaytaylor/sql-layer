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
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.*;
import com.akiban.server.api.dml.scan.NewRow;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

import static com.akiban.qp.operator.API.FlattenOption.*;
import static com.akiban.qp.operator.API.JoinType.*;
import static com.akiban.qp.operator.API.LookupOption.*;
import static com.akiban.qp.operator.API.*;
import static org.junit.Assert.assertTrue;

// Product_ByRun relies on run ids. Here is an explanation of why run ids are needed:
// http://akibainc.onconfluence.com/display/db/Implementation+of+the+Product+operator.
// But no other operator relies on runs, and run ids can probably be omitted if we implement
// a nested-loops form of product. This test uses a 3-way product, the case motivating run ids.

public class Product3WayIT extends OperatorITBase
{
    @Before
    public void before()
    {
        // Don't call super.before(). This is a different schema from most operator ITs.
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
        queryContext = queryContext(adapter);
        use(db);
    }

    // Test assumption about ordinals

    @Test
    public void testOrdinalOrder()
    {
        assertTrue(ordinal(rRowType) < ordinal(aRowType));
        assertTrue(ordinal(aRowType) < ordinal(bRowType));
        assertTrue(ordinal(bRowType) < ordinal(cRowType));
    }

    // Test operator execution

    public void testProductAfterIndexScanOfA_NestedLoops_RABC()
    {
        Operator RA =
            flatten_HKeyOrdered(
                ancestorLookup_Default(
                    indexScan_Default(aValueIndexRowType, false),
                    rabc,
                    aValueIndexRowType,
                    Arrays.asList(aRowType, rRowType),
                    DISCARD_INPUT),
                rRowType,
                aRowType,
                INNER_JOIN);
        Operator RB =
            flatten_HKeyOrdered(
                branchLookup_Nested(
                    rabc,
                    rRowType,
                    bRowType,
                    KEEP_INPUT,
                    0),
                rRowType,
                bRowType,
                INNER_JOIN);
        Operator RC =
            flatten_HKeyOrdered(
                branchLookup_Nested(
                    rabc,
                    rRowType,
                    cRowType,
                    KEEP_INPUT,
                    0),
                rRowType,
                cRowType,
                INNER_JOIN);
        Operator RAB = product_NestedLoops(RA, RB, RA.rowType(), RB.rowType(), 0);
        Operator RABC = product_NestedLoops(RAB, RC, RAB.rowType(), RC.rowType(), 0);
        Cursor cursor = cursor(RABC, queryContext);
        RowType rabcRowType = RABC.rowType();
        RowBase[] expected = new RowBase[]{
            row(rabcRowType, 1L, "r1", 13L, 1L, "a13", 15L, 1L, "b15", 17L, 1L, "c17"),
            row(rabcRowType, 1L, "r1", 13L, 1L, "a13", 15L, 1L, "b15", 18L, 1L, "c18"),
            row(rabcRowType, 1L, "r1", 13L, 1L, "a13", 16L, 1L, "b16", 17L, 1L, "c17"),
            row(rabcRowType, 1L, "r1", 13L, 1L, "a13", 16L, 1L, "b16", 18L, 1L, "c18"),
            row(rabcRowType, 1L, "r1", 14L, 1L, "a14", 15L, 1L, "b15", 17L, 1L, "c17"),
            row(rabcRowType, 1L, "r1", 14L, 1L, "a14", 15L, 1L, "b15", 18L, 1L, "c18"),
            row(rabcRowType, 1L, "r1", 14L, 1L, "a14", 16L, 1L, "b16", 17L, 1L, "c17"),
            row(rabcRowType, 1L, "r1", 14L, 1L, "a14", 16L, 1L, "b16", 18L, 1L, "c18"),
            row(rabcRowType, 2L, "r2", 23L, 2L, "a23", 25L, 2L, "b25", 27L, 2L, "c27"),
            row(rabcRowType, 2L, "r2", 23L, 2L, "a23", 25L, 2L, "b25", 28L, 2L, "c28"),
            row(rabcRowType, 2L, "r2", 23L, 2L, "a23", 26L, 2L, "b26", 27L, 2L, "c27"),
            row(rabcRowType, 2L, "r2", 23L, 2L, "a23", 26L, 2L, "b26", 28L, 2L, "c28"),
            row(rabcRowType, 2L, "r2", 24L, 2L, "a24", 25L, 2L, "b25", 27L, 2L, "c27"),
            row(rabcRowType, 2L, "r2", 24L, 2L, "a24", 25L, 2L, "b25", 28L, 2L, "c28"),
            row(rabcRowType, 2L, "r2", 24L, 2L, "a24", 26L, 2L, "b26", 27L, 2L, "c27"),
            row(rabcRowType, 2L, "r2", 24L, 2L, "a24", 26L, 2L, "b26", 28L, 2L, "c28"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testProductAfterIndexScanOfA_NestedLoops_RACB()
    {
        // Like testProductAfterIndexScanOfA_NestedLoops_RABC, but branches are included in a different order.
        Operator RA =
            flatten_HKeyOrdered(
                ancestorLookup_Default(
                    indexScan_Default(aValueIndexRowType, false),
                    rabc,
                    aValueIndexRowType,
                    Arrays.asList(aRowType, rRowType),
                    DISCARD_INPUT),
                rRowType,
                aRowType,
                INNER_JOIN);
        Operator RB =
            flatten_HKeyOrdered(
                branchLookup_Nested(
                    rabc,
                    rRowType,
                    bRowType,
                    KEEP_INPUT,
                    0),
                rRowType,
                bRowType,
                INNER_JOIN);
        Operator RC =
            flatten_HKeyOrdered(
                branchLookup_Nested(
                    rabc,
                    rRowType,
                    cRowType,
                    KEEP_INPUT,
                    0),
                rRowType,
                cRowType,
                INNER_JOIN);
        Operator RAC = product_NestedLoops(RA, RC, RA.rowType(), RC.rowType(), 0);
        Operator RACB = product_NestedLoops(RAC, RB, RAC.rowType(), RB.rowType(), 0);
        Cursor cursor = cursor(RACB, queryContext);
        RowType racbRowType = RACB.rowType();
        RowBase[] expected = new RowBase[]{
            row(racbRowType, 1L, "r1", 13L, 1L, "a13", 17L, 1L, "c17", 15L, 1L, "b15"),
            row(racbRowType, 1L, "r1", 13L, 1L, "a13", 17L, 1L, "c17", 16L, 1L, "b16"),
            row(racbRowType, 1L, "r1", 13L, 1L, "a13", 18L, 1L, "c18", 15L, 1L, "b15"),
            row(racbRowType, 1L, "r1", 13L, 1L, "a13", 18L, 1L, "c18", 16L, 1L, "b16"),
            row(racbRowType, 1L, "r1", 14L, 1L, "a14", 17L, 1L, "c17", 15L, 1L, "b15"),
            row(racbRowType, 1L, "r1", 14L, 1L, "a14", 17L, 1L, "c17", 16L, 1L, "b16"),
            row(racbRowType, 1L, "r1", 14L, 1L, "a14", 18L, 1L, "c18", 15L, 1L, "b15"),
            row(racbRowType, 1L, "r1", 14L, 1L, "a14", 18L, 1L, "c18", 16L, 1L, "b16"),
            row(racbRowType, 2L, "r2", 23L, 2L, "a23", 27L, 2L, "c27", 25L, 2L, "b25"),
            row(racbRowType, 2L, "r2", 23L, 2L, "a23", 27L, 2L, "c27", 26L, 2L, "b26"),
            row(racbRowType, 2L, "r2", 23L, 2L, "a23", 28L, 2L, "c28", 25L, 2L, "b25"),
            row(racbRowType, 2L, "r2", 23L, 2L, "a23", 28L, 2L, "c28", 26L, 2L, "b26"),
            row(racbRowType, 2L, "r2", 24L, 2L, "a24", 27L, 2L, "c27", 25L, 2L, "b25"),
            row(racbRowType, 2L, "r2", 24L, 2L, "a24", 27L, 2L, "c27", 26L, 2L, "b26"),
            row(racbRowType, 2L, "r2", 24L, 2L, "a24", 28L, 2L, "c28", 25L, 2L, "b25"),
            row(racbRowType, 2L, "r2", 24L, 2L, "a24", 28L, 2L, "c28", 26L, 2L, "b26"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testProductAfterIndexScanOfR()
    {
        Operator rScan =
            ancestorLookup_Default(
                indexScan_Default(rValueIndexRowType, false),
                rabc,
                rValueIndexRowType,
                Arrays.asList(rRowType),
                DISCARD_INPUT);
        Operator flattenRA =
            flatten_HKeyOrdered(
                branchLookup_Nested(
                    rabc,
                    rRowType,
                    aRowType,
                    KEEP_INPUT,
                    0),
                rRowType,
                aRowType,
                INNER_JOIN);
        Operator flattenRB =
            flatten_HKeyOrdered(
                branchLookup_Nested(
                    rabc,
                    rRowType,
                    bRowType,
                    KEEP_INPUT,
                    0),
                rRowType,
                bRowType,
                INNER_JOIN);
        Operator flattenRC =
            flatten_HKeyOrdered(
                branchLookup_Nested(
                    rabc,
                    rRowType,
                    cRowType,
                    KEEP_INPUT,
                    0),
                rRowType,
                cRowType,
                INNER_JOIN);
        Operator RA = product_NestedLoops(rScan, flattenRA, rRowType, flattenRA.rowType(), 0);
        Operator RAB = product_NestedLoops(RA, flattenRB, RA.rowType(), flattenRB.rowType(), 0);
        Operator RABC = product_NestedLoops(RAB, flattenRC, RAB.rowType(), flattenRC.rowType(), 0);
        Cursor cursor = cursor(RABC, queryContext);
        RowType rabcRowType = RABC.rowType();
        RowBase[] expected = new RowBase[]{
            row(rabcRowType, 1L, "r1", 13L, 1L, "a13", 15L, 1L, "b15", 17L, 1L, "c17"),
            row(rabcRowType, 1L, "r1", 13L, 1L, "a13", 15L, 1L, "b15", 18L, 1L, "c18"),
            row(rabcRowType, 1L, "r1", 13L, 1L, "a13", 16L, 1L, "b16", 17L, 1L, "c17"),
            row(rabcRowType, 1L, "r1", 13L, 1L, "a13", 16L, 1L, "b16", 18L, 1L, "c18"),
            row(rabcRowType, 1L, "r1", 14L, 1L, "a14", 15L, 1L, "b15", 17L, 1L, "c17"),
            row(rabcRowType, 1L, "r1", 14L, 1L, "a14", 15L, 1L, "b15", 18L, 1L, "c18"),
            row(rabcRowType, 1L, "r1", 14L, 1L, "a14", 16L, 1L, "b16", 17L, 1L, "c17"),
            row(rabcRowType, 1L, "r1", 14L, 1L, "a14", 16L, 1L, "b16", 18L, 1L, "c18"),
            row(rabcRowType, 2L, "r2", 23L, 2L, "a23", 25L, 2L, "b25", 27L, 2L, "c27"),
            row(rabcRowType, 2L, "r2", 23L, 2L, "a23", 25L, 2L, "b25", 28L, 2L, "c28"),
            row(rabcRowType, 2L, "r2", 23L, 2L, "a23", 26L, 2L, "b26", 27L, 2L, "c27"),
            row(rabcRowType, 2L, "r2", 23L, 2L, "a23", 26L, 2L, "b26", 28L, 2L, "c28"),
            row(rabcRowType, 2L, "r2", 24L, 2L, "a24", 25L, 2L, "b25", 27L, 2L, "c27"),
            row(rabcRowType, 2L, "r2", 24L, 2L, "a24", 25L, 2L, "b25", 28L, 2L, "c28"),
            row(rabcRowType, 2L, "r2", 24L, 2L, "a24", 26L, 2L, "b26", 27L, 2L, "c27"),
            row(rabcRowType, 2L, "r2", 24L, 2L, "a24", 26L, 2L, "b26", 28L, 2L, "c28"),
        };
        compareRows(expected, cursor);
    }

    // TODO: Test handling of rows whose type is not involved in product.

    private Set<AisRowType> removeDescendentTypes(AisRowType type)
    {
        Set<AisRowType> keepTypes = type.schema().userTableTypes();
        keepTypes.removeAll(Schema.descendentTypes(type, keepTypes));
        return keepTypes;
    }

    protected int r;
    protected int a;
    protected int c;
    protected int b;
    protected UserTableRowType rRowType;
    protected UserTableRowType aRowType;
    protected UserTableRowType cRowType;
    protected UserTableRowType bRowType;
    protected IndexRowType aValueIndexRowType;
    protected IndexRowType rValueIndexRowType;
    protected GroupTable rabc;
}
