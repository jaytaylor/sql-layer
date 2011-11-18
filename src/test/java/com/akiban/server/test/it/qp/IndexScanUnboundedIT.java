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

import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.persistitadapter.OperatorStore;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.expression.std.FieldExpression;
import com.akiban.server.store.PersistitStore;
import com.akiban.server.store.Store;
import org.junit.Before;
import org.junit.Test;

import static com.akiban.qp.operator.API.cursor;
import static com.akiban.qp.operator.API.indexScan_Default;

/*
 * This test covers unbounded index scans with combinations of the following variations:
 * - ascending/descending/mixed
 * - order covers all/some key fields
 * - null values
 */

public class IndexScanUnboundedIT extends OperatorITBase
{
    @Before
    public void before()
    {
        t = createTable(
            "schema", "t",
            "id int not null key",
            "a int",
            "b int",
            "c int",
            "index(a, b, c, id)");
        schema = new Schema(rowDefCache().ais());
        tRowType = schema.userTableRowType(userTable(t));
        idxRowType = indexType(t, "a", "b", "c", "id");
        db = new NewRow[]{
            // No nulls
            createNewRow(t, 1000L, 1L, 11L, 111L),
            createNewRow(t, 1001L, 1L, 11L, 112L),
            createNewRow(t, 1002L, 1L, 12L, 121L),
            createNewRow(t, 1003L, 1L, 12L, 122L),
            createNewRow(t, 1004L, 2L, 21L, 211L),
            createNewRow(t, 1005L, 2L, 21L, 212L),
            createNewRow(t, 1006L, 2L, 22L, 221L),
            createNewRow(t, 1007L, 2L, 22L, 222L),
            // With nulls
            createNewRow(t, 2000L, 3L, 4L, 5L),
            createNewRow(t, 2001L, 3L, 4L, null),
            createNewRow(t, 2002L, 3L, null, 5L),
            createNewRow(t, 2003L, 3L, null, null),
            createNewRow(t, 2004L, null, 4L, 5L),
            createNewRow(t, 2005L, null, 4L, null),
            createNewRow(t, 2006L, null, null, 5L),
            createNewRow(t, 2007L, null, null, null),
        };
        Store plainStore = store();
        final PersistitStore persistitStore;
        if (plainStore instanceof OperatorStore) {
            OperatorStore operatorStore = (OperatorStore) plainStore;
            persistitStore = operatorStore.getPersistitStore();
        } else {
            persistitStore = (PersistitStore) plainStore;
        }
        adapter = new PersistitAdapter(schema, persistitStore, null, session(), configService());
        use(db);
    }

    // Test name: testXYZ_DESCRIPTION
    // X: Unbounded/Bounded
    // Y: Asc/Desc/Mixed
    // Z: All/Some key fields included in ordering
    // DESCRIPTION: description of test case

    @Test
    public void testAscAll()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, ASC, B, ASC, C, ASC, ID, ASC));
        RowBase[] expected = new RowBase[]{
            row(idxRowType, null, null, null, 2007L),
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, null, 4L, 5L, 2004L),
            row(idxRowType, 1L, 11L, 111L, 1000L),
            row(idxRowType, 1L, 11L, 112L, 1001L),
            row(idxRowType, 1L, 12L, 121L, 1002L),
            row(idxRowType, 1L, 12L, 122L, 1003L),
            row(idxRowType, 2L, 21L, 211L, 1004L),
            row(idxRowType, 2L, 21L, 212L, 1005L),
            row(idxRowType, 2L, 22L, 221L, 1006L),
            row(idxRowType, 2L, 22L, 222L, 1007L),
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 3L, null, 5L, 2002L),
            row(idxRowType, 3L, 4L, null, 2001L),
            row(idxRowType, 3L, 4L, 5L, 2000L),
        };
        compareRows(expected, cursor(plan, adapter));
    }

    @Test
    public void testAscSome_ABC()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, ASC, B, ASC, C, ASC));
        RowBase[] expected = new RowBase[]{
            row(idxRowType, null, null, null, 2007L),
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, null, 4L, 5L, 2004L),
            row(idxRowType, 1L, 11L, 111L, 1000L),
            row(idxRowType, 1L, 11L, 112L, 1001L),
            row(idxRowType, 1L, 12L, 121L, 1002L),
            row(idxRowType, 1L, 12L, 122L, 1003L),
            row(idxRowType, 2L, 21L, 211L, 1004L),
            row(idxRowType, 2L, 21L, 212L, 1005L),
            row(idxRowType, 2L, 22L, 221L, 1006L),
            row(idxRowType, 2L, 22L, 222L, 1007L),
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 3L, null, 5L, 2002L),
            row(idxRowType, 3L, 4L, null, 2001L),
            row(idxRowType, 3L, 4L, 5L, 2000L),
        };
        compareRows(expected, cursor(plan, adapter));
    }

    @Test
    public void testAscSome_AB()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, ASC, B, ASC));
        RowBase[] expected = new RowBase[]{
            row(idxRowType, null, null, null, 2007L),
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, null, 4L, 5L, 2004L),
            row(idxRowType, 1L, 11L, 111L, 1000L),
            row(idxRowType, 1L, 11L, 112L, 1001L),
            row(idxRowType, 1L, 12L, 121L, 1002L),
            row(idxRowType, 1L, 12L, 122L, 1003L),
            row(idxRowType, 2L, 21L, 211L, 1004L),
            row(idxRowType, 2L, 21L, 212L, 1005L),
            row(idxRowType, 2L, 22L, 221L, 1006L),
            row(idxRowType, 2L, 22L, 222L, 1007L),
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 3L, null, 5L, 2002L),
            row(idxRowType, 3L, 4L, null, 2001L),
            row(idxRowType, 3L, 4L, 5L, 2000L),
        };
        compareRows(expected, cursor(plan, adapter));
    }

    @Test
    public void testAscSome_A()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, ASC));
        RowBase[] expected = new RowBase[]{
            row(idxRowType, null, null, null, 2007L),
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, null, 4L, 5L, 2004L),
            row(idxRowType, 1L, 11L, 111L, 1000L),
            row(idxRowType, 1L, 11L, 112L, 1001L),
            row(idxRowType, 1L, 12L, 121L, 1002L),
            row(idxRowType, 1L, 12L, 122L, 1003L),
            row(idxRowType, 2L, 21L, 211L, 1004L),
            row(idxRowType, 2L, 21L, 212L, 1005L),
            row(idxRowType, 2L, 22L, 221L, 1006L),
            row(idxRowType, 2L, 22L, 222L, 1007L),
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 3L, null, 5L, 2002L),
            row(idxRowType, 3L, 4L, null, 2001L),
            row(idxRowType, 3L, 4L, 5L, 2000L),
        };
        compareRows(expected, cursor(plan, adapter));
    }

    @Test
    public void testDescAll()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, DESC, B, DESC, C, DESC, ID, DESC));
        RowBase[] expected = new RowBase[]{
            row(idxRowType, 3L, 4L, 5L, 2000L),
            row(idxRowType, 3L, 4L, null, 2001L),
            row(idxRowType, 3L, null, 5L, 2002L),
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 2L, 22L, 222L, 1007L),
            row(idxRowType, 2L, 22L, 221L, 1006L),
            row(idxRowType, 2L, 21L, 212L, 1005L),
            row(idxRowType, 2L, 21L, 211L, 1004L),
            row(idxRowType, 1L, 12L, 122L, 1003L),
            row(idxRowType, 1L, 12L, 121L, 1002L),
            row(idxRowType, 1L, 11L, 112L, 1001L),
            row(idxRowType, 1L, 11L, 111L, 1000L),
            row(idxRowType, null, 4L, 5L, 2004L),
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, null, null, null, 2007L),
        };
        compareRows(expected, cursor(plan, adapter));
    }

    @Test
    public void testDescSome_ABC()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, DESC, B, DESC, C, DESC));
        RowBase[] expected = new RowBase[]{
            row(idxRowType, 3L, 4L, 5L, 2000L),
            row(idxRowType, 3L, 4L, null, 2001L),
            row(idxRowType, 3L, null, 5L, 2002L),
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 2L, 22L, 222L, 1007L),
            row(idxRowType, 2L, 22L, 221L, 1006L),
            row(idxRowType, 2L, 21L, 212L, 1005L),
            row(idxRowType, 2L, 21L, 211L, 1004L),
            row(idxRowType, 1L, 12L, 122L, 1003L),
            row(idxRowType, 1L, 12L, 121L, 1002L),
            row(idxRowType, 1L, 11L, 112L, 1001L),
            row(idxRowType, 1L, 11L, 111L, 1000L),
            row(idxRowType, null, 4L, 5L, 2004L),
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, null, null, null, 2007L),
        };
        compareRows(expected, cursor(plan, adapter));
    }

    @Test
    public void testDescSome_AB()
    {
        // All specified orderings are DESC, so a unidirectional traversal is done. That's why everything
        // is in reverse order, not just the columns listed explicitly in the ordering.
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, DESC, B, DESC));
        RowBase[] expected = new RowBase[]{
            row(idxRowType, 3L, 4L, 5L, 2000L),
            row(idxRowType, 3L, 4L, null, 2001L),
            row(idxRowType, 3L, null, 5L, 2002L),
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 2L, 22L, 222L, 1007L),
            row(idxRowType, 2L, 22L, 221L, 1006L),
            row(idxRowType, 2L, 21L, 212L, 1005L),
            row(idxRowType, 2L, 21L, 211L, 1004L),
            row(idxRowType, 1L, 12L, 122L, 1003L),
            row(idxRowType, 1L, 12L, 121L, 1002L),
            row(idxRowType, 1L, 11L, 112L, 1001L),
            row(idxRowType, 1L, 11L, 111L, 1000L),
            row(idxRowType, null, 4L, 5L, 2004L),
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, null, null, null, 2007L),
        };
        compareRows(expected, cursor(plan, adapter));
    }

    @Test
    public void testDescSome_A()
    {
        // All specified orderings are DESC, so a unidirectional traversal is done. That's why everything
        // is in reverse order, not just the columns listed explicitly in the ordering.
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, DESC));
        RowBase[] expected = new RowBase[]{
            row(idxRowType, 3L, 4L, 5L, 2000L),
            row(idxRowType, 3L, 4L, null, 2001L),
            row(idxRowType, 3L, null, 5L, 2002L),
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 2L, 22L, 222L, 1007L),
            row(idxRowType, 2L, 22L, 221L, 1006L),
            row(idxRowType, 2L, 21L, 212L, 1005L),
            row(idxRowType, 2L, 21L, 211L, 1004L),
            row(idxRowType, 1L, 12L, 122L, 1003L),
            row(idxRowType, 1L, 12L, 121L, 1002L),
            row(idxRowType, 1L, 11L, 112L, 1001L),
            row(idxRowType, 1L, 11L, 111L, 1000L),
            row(idxRowType, null, 4L, 5L, 2004L),
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, null, null, null, 2007L),
        };
        compareRows(expected, cursor(plan, adapter));
    }

    @Test
    public void testMixedAll_AADA()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, ASC, B, ASC, C, DESC, ID, ASC));
        RowBase[] expected = new RowBase[]{
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, null, null, null, 2007L),
            row(idxRowType, null, 4L, 5L, 2004L),
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, 1L, 11L, 112L, 1001L),
            row(idxRowType, 1L, 11L, 111L, 1000L),
            row(idxRowType, 1L, 12L, 122L, 1003L),
            row(idxRowType, 1L, 12L, 121L, 1002L),
            row(idxRowType, 2L, 21L, 212L, 1005L),
            row(idxRowType, 2L, 21L, 211L, 1004L),
            row(idxRowType, 2L, 22L, 222L, 1007L),
            row(idxRowType, 2L, 22L, 221L, 1006L),
            row(idxRowType, 3L, null, 5L, 2002L),
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 3L, 4L, 5L, 2000L),
            row(idxRowType, 3L, 4L, null, 2001L),
        };
        compareRows(expected, cursor(plan, adapter));
    }

    @Test
    public void testMixedSome_AAD()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, ASC, B, ASC, C, DESC));
        RowBase[] expected = new RowBase[]{
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, null, null, null, 2007L),
            row(idxRowType, null, 4L, 5L, 2004L),
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, 1L, 11L, 112L, 1001L),
            row(idxRowType, 1L, 11L, 111L, 1000L),
            row(idxRowType, 1L, 12L, 122L, 1003L),
            row(idxRowType, 1L, 12L, 121L, 1002L),
            row(idxRowType, 2L, 21L, 212L, 1005L),
            row(idxRowType, 2L, 21L, 211L, 1004L),
            row(idxRowType, 2L, 22L, 222L, 1007L),
            row(idxRowType, 2L, 22L, 221L, 1006L),
            row(idxRowType, 3L, null, 5L, 2002L),
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 3L, 4L, 5L, 2000L),
            row(idxRowType, 3L, 4L, null, 2001L),
        };
        compareRows(expected, cursor(plan, adapter));
    }

    @Test
    public void testMixedAll_ADAA()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, ASC, B, DESC, C, ASC, ID, ASC));
        RowBase[] expected = new RowBase[]{
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, null, 4L, 5L, 2004L),
            row(idxRowType, null, null, null, 2007L),
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, 1L, 12L, 121L, 1002L),
            row(idxRowType, 1L, 12L, 122L, 1003L),
            row(idxRowType, 1L, 11L, 111L, 1000L),
            row(idxRowType, 1L, 11L, 112L, 1001L),
            row(idxRowType, 2L, 22L, 221L, 1006L),
            row(idxRowType, 2L, 22L, 222L, 1007L),
            row(idxRowType, 2L, 21L, 211L, 1004L),
            row(idxRowType, 2L, 21L, 212L, 1005L),
            row(idxRowType, 3L, 4L, null, 2001L),
            row(idxRowType, 3L, 4L, 5L, 2000L),
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 3L, null, 5L, 2002L),
        };
        compareRows(expected, cursor(plan, adapter));
    }

    @Test
    public void testMixedSome_ADA()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, ASC, B, DESC, C, ASC));
        RowBase[] expected = new RowBase[]{
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, null, 4L, 5L, 2004L),
            row(idxRowType, null, null, null, 2007L),
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, 1L, 12L, 121L, 1002L),
            row(idxRowType, 1L, 12L, 122L, 1003L),
            row(idxRowType, 1L, 11L, 111L, 1000L),
            row(idxRowType, 1L, 11L, 112L, 1001L),
            row(idxRowType, 2L, 22L, 221L, 1006L),
            row(idxRowType, 2L, 22L, 222L, 1007L),
            row(idxRowType, 2L, 21L, 211L, 1004L),
            row(idxRowType, 2L, 21L, 212L, 1005L),
            row(idxRowType, 3L, 4L, null, 2001L),
            row(idxRowType, 3L, 4L, 5L, 2000L),
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 3L, null, 5L, 2002L),
        };
        compareRows(expected, cursor(plan, adapter));
    }

    @Test
    public void testMixedSome_AD()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, ASC, B, DESC));
        RowBase[] expected = new RowBase[]{
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, null, 4L, 5L, 2004L),
            row(idxRowType, null, null, null, 2007L),
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, 1L, 12L, 121L, 1002L),
            row(idxRowType, 1L, 12L, 122L, 1003L),
            row(idxRowType, 1L, 11L, 111L, 1000L),
            row(idxRowType, 1L, 11L, 112L, 1001L),
            row(idxRowType, 2L, 22L, 221L, 1006L),
            row(idxRowType, 2L, 22L, 222L, 1007L),
            row(idxRowType, 2L, 21L, 211L, 1004L),
            row(idxRowType, 2L, 21L, 212L, 1005L),
            row(idxRowType, 3L, 4L, null, 2001L),
            row(idxRowType, 3L, 4L, 5L, 2000L),
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 3L, null, 5L, 2002L),
        };
        compareRows(expected, cursor(plan, adapter));
    }

    @Test
    public void testMixedAll_ADDA()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, ASC, B, DESC, C, DESC, ID, ASC));
        RowBase[] expected = new RowBase[]{
            row(idxRowType, null, 4L, 5L, 2004L),
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, null, null, null, 2007L),
            row(idxRowType, 1L, 12L, 122L, 1003L),
            row(idxRowType, 1L, 12L, 121L, 1002L),
            row(idxRowType, 1L, 11L, 112L, 1001L),
            row(idxRowType, 1L, 11L, 111L, 1000L),
            row(idxRowType, 2L, 22L, 222L, 1007L),
            row(idxRowType, 2L, 22L, 221L, 1006L),
            row(idxRowType, 2L, 21L, 212L, 1005L),
            row(idxRowType, 2L, 21L, 211L, 1004L),
            row(idxRowType, 3L, 4L, 5L, 2000L),
            row(idxRowType, 3L, 4L, null, 2001L),
            row(idxRowType, 3L, null, 5L, 2002L),
            row(idxRowType, 3L, null, null, 2003L),
        };
        compareRows(expected, cursor(plan, adapter));
    }

    @Test
    public void testMixedSome_ADD()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, ASC, B, DESC, C, DESC));
        RowBase[] expected = new RowBase[]{
            row(idxRowType, null, 4L, 5L, 2004L),
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, null, null, null, 2007L),
            row(idxRowType, 1L, 12L, 122L, 1003L),
            row(idxRowType, 1L, 12L, 121L, 1002L),
            row(idxRowType, 1L, 11L, 112L, 1001L),
            row(idxRowType, 1L, 11L, 111L, 1000L),
            row(idxRowType, 2L, 22L, 222L, 1007L),
            row(idxRowType, 2L, 22L, 221L, 1006L),
            row(idxRowType, 2L, 21L, 212L, 1005L),
            row(idxRowType, 2L, 21L, 211L, 1004L),
            row(idxRowType, 3L, 4L, 5L, 2000L),
            row(idxRowType, 3L, 4L, null, 2001L),
            row(idxRowType, 3L, null, 5L, 2002L),
            row(idxRowType, 3L, null, null, 2003L),
        };
        compareRows(expected, cursor(plan, adapter));
    }

    @Test
    public void testMixedAll_DAAA()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, DESC, B, ASC, C, ASC, ID, ASC));
        RowBase[] expected = new RowBase[]{
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 3L, null, 5L, 2002L),
            row(idxRowType, 3L, 4L, null, 2001L),
            row(idxRowType, 3L, 4L, 5L, 2000L),
            row(idxRowType, 2L, 21L, 211L, 1004L),
            row(idxRowType, 2L, 21L, 212L, 1005L),
            row(idxRowType, 2L, 22L, 221L, 1006L),
            row(idxRowType, 2L, 22L, 222L, 1007L),
            row(idxRowType, 1L, 11L, 111L, 1000L),
            row(idxRowType, 1L, 11L, 112L, 1001L),
            row(idxRowType, 1L, 12L, 121L, 1002L),
            row(idxRowType, 1L, 12L, 122L, 1003L),
            row(idxRowType, null, null, null, 2007L),
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, null, 4L, 5L, 2004L),
        };
        compareRows(expected, cursor(plan, adapter));
    }

    @Test
    public void testMixedSome_DAA()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, DESC, B, ASC, C, ASC));
        RowBase[] expected = new RowBase[]{
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 3L, null, 5L, 2002L),
            row(idxRowType, 3L, 4L, null, 2001L),
            row(idxRowType, 3L, 4L, 5L, 2000L),
            row(idxRowType, 2L, 21L, 211L, 1004L),
            row(idxRowType, 2L, 21L, 212L, 1005L),
            row(idxRowType, 2L, 22L, 221L, 1006L),
            row(idxRowType, 2L, 22L, 222L, 1007L),
            row(idxRowType, 1L, 11L, 111L, 1000L),
            row(idxRowType, 1L, 11L, 112L, 1001L),
            row(idxRowType, 1L, 12L, 121L, 1002L),
            row(idxRowType, 1L, 12L, 122L, 1003L),
            row(idxRowType, null, null, null, 2007L),
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, null, 4L, 5L, 2004L),
        };
        compareRows(expected, cursor(plan, adapter));
    }

    @Test
    public void testMixedSome_DA()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, DESC, B, ASC));
        RowBase[] expected = new RowBase[]{
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 3L, null, 5L, 2002L),
            row(idxRowType, 3L, 4L, null, 2001L),
            row(idxRowType, 3L, 4L, 5L, 2000L),
            row(idxRowType, 2L, 21L, 211L, 1004L),
            row(idxRowType, 2L, 21L, 212L, 1005L),
            row(idxRowType, 2L, 22L, 221L, 1006L),
            row(idxRowType, 2L, 22L, 222L, 1007L),
            row(idxRowType, 1L, 11L, 111L, 1000L),
            row(idxRowType, 1L, 11L, 112L, 1001L),
            row(idxRowType, 1L, 12L, 121L, 1002L),
            row(idxRowType, 1L, 12L, 122L, 1003L),
            row(idxRowType, null, null, null, 2007L),
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, null, 4L, 5L, 2004L),
        };
        compareRows(expected, cursor(plan, adapter));
    }

    @Test
    public void testMixedAll_DADA()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, DESC, B, ASC, C, DESC, ID, ASC));
        RowBase[] expected = new RowBase[]{
            row(idxRowType, 3L, null, 5L, 2002L),
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 3L, 4L, 5L, 2000L),
            row(idxRowType, 3L, 4L, null, 2001L),
            row(idxRowType, 2L, 21L, 212L, 1005L),
            row(idxRowType, 2L, 21L, 211L, 1004L),
            row(idxRowType, 2L, 22L, 222L, 1007L),
            row(idxRowType, 2L, 22L, 221L, 1006L),
            row(idxRowType, 1L, 11L, 112L, 1001L),
            row(idxRowType, 1L, 11L, 111L, 1000L),
            row(idxRowType, 1L, 12L, 122L, 1003L),
            row(idxRowType, 1L, 12L, 121L, 1002L),
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, null, null, null, 2007L),
            row(idxRowType, null, 4L, 5L, 2004L),
            row(idxRowType, null, 4L, null, 2005L),
        };
        compareRows(expected, cursor(plan, adapter));
    }

    @Test
    public void testMixedSome_DAD()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, DESC, B, ASC, C, DESC));
        RowBase[] expected = new RowBase[]{
            row(idxRowType, 3L, null, 5L, 2002L),
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 3L, 4L, 5L, 2000L),
            row(idxRowType, 3L, 4L, null, 2001L),
            row(idxRowType, 2L, 21L, 212L, 1005L),
            row(idxRowType, 2L, 21L, 211L, 1004L),
            row(idxRowType, 2L, 22L, 222L, 1007L),
            row(idxRowType, 2L, 22L, 221L, 1006L),
            row(idxRowType, 1L, 11L, 112L, 1001L),
            row(idxRowType, 1L, 11L, 111L, 1000L),
            row(idxRowType, 1L, 12L, 122L, 1003L),
            row(idxRowType, 1L, 12L, 121L, 1002L),
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, null, null, null, 2007L),
            row(idxRowType, null, 4L, 5L, 2004L),
            row(idxRowType, null, 4L, null, 2005L),
        };
        compareRows(expected, cursor(plan, adapter));
    }

    @Test
    public void testMixedAll_DDAA()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, DESC, B, DESC, C, ASC, ID, ASC));
        RowBase[] expected = new RowBase[]{
            row(idxRowType, 3L, 4L, null, 2001L),
            row(idxRowType, 3L, 4L, 5L, 2000L),
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 3L, null, 5L, 2002L),
            row(idxRowType, 2L, 22L, 221L, 1006L),
            row(idxRowType, 2L, 22L, 222L, 1007L),
            row(idxRowType, 2L, 21L, 211L, 1004L),
            row(idxRowType, 2L, 21L, 212L, 1005L),
            row(idxRowType, 1L, 12L, 121L, 1002L),
            row(idxRowType, 1L, 12L, 122L, 1003L),
            row(idxRowType, 1L, 11L, 111L, 1000L),
            row(idxRowType, 1L, 11L, 112L, 1001L),
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, null, 4L, 5L, 2004L),
            row(idxRowType, null, null, null, 2007L),
            row(idxRowType, null, null, 5L, 2006L),
        };
        compareRows(expected, cursor(plan, adapter));
    }

    @Test
    public void testMixedSome_DDA()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, DESC, B, DESC, C, ASC));
        RowBase[] expected = new RowBase[]{
            row(idxRowType, 3L, 4L, null, 2001L),
            row(idxRowType, 3L, 4L, 5L, 2000L),
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 3L, null, 5L, 2002L),
            row(idxRowType, 2L, 22L, 221L, 1006L),
            row(idxRowType, 2L, 22L, 222L, 1007L),
            row(idxRowType, 2L, 21L, 211L, 1004L),
            row(idxRowType, 2L, 21L, 212L, 1005L),
            row(idxRowType, 1L, 12L, 121L, 1002L),
            row(idxRowType, 1L, 12L, 122L, 1003L),
            row(idxRowType, 1L, 11L, 111L, 1000L),
            row(idxRowType, 1L, 11L, 112L, 1001L),
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, null, 4L, 5L, 2004L),
            row(idxRowType, null, null, null, 2007L),
            row(idxRowType, null, null, 5L, 2006L),
        };
        compareRows(expected, cursor(plan, adapter));
    }

    @Test
    public void testMixedAll_DDDA()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, DESC, B, DESC, C, DESC, ID, ASC));
        RowBase[] expected = new RowBase[]{
            row(idxRowType, 3L, 4L, 5L, 2000L),
            row(idxRowType, 3L, 4L, null, 2001L),
            row(idxRowType, 3L, null, 5L, 2002L),
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 2L, 22L, 222L, 1007L),
            row(idxRowType, 2L, 22L, 221L, 1006L),
            row(idxRowType, 2L, 21L, 212L, 1005L),
            row(idxRowType, 2L, 21L, 211L, 1004L),
            row(idxRowType, 1L, 12L, 122L, 1003L),
            row(idxRowType, 1L, 12L, 121L, 1002L),
            row(idxRowType, 1L, 11L, 112L, 1001L),
            row(idxRowType, 1L, 11L, 111L, 1000L),
            row(idxRowType, null, 4L, 5L, 2004L),
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, null, null, null, 2007L),
        };
        compareRows(expected, cursor(plan, adapter));
    }

    // For use by this class

    private IndexKeyRange unbounded()
    {
        return IndexKeyRange.unbounded(idxRowType);
    }
    
    private API.Ordering ordering(Object ... ord) // alternating column positions and asc/desc
    {
        API.Ordering ordering = API.ordering();
        int i = 0;
        while (i < ord.length) {
            int column = (Integer) ord[i++];
            boolean asc = (Boolean) ord[i++];
            ordering.append(new FieldExpression(idxRowType, column), asc);
        }
        return ordering;
    }

    // Positions of fields within the index row
    private static final int A = 0;
    private static final int B = 1;
    private static final int C = 2;
    private static final int ID = 3;
    private static final boolean ASC = true;
    private static final boolean DESC = false;

    private int t;
    private RowType tRowType;
    private IndexRowType idxRowType;
}
