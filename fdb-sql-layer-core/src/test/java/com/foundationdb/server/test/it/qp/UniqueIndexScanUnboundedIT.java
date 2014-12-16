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

import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import org.junit.Test;

import static com.foundationdb.qp.operator.API.cursor;
import static com.foundationdb.qp.operator.API.indexScan_Default;
import static com.foundationdb.server.test.ExpressionGenerators.field;

/*
 * This test covers unbounded index scans with combinations of the following variations:
 * - ascending/descending/mixed
 * - order covers all/some key fields
 * - null values
 */

public class UniqueIndexScanUnboundedIT extends OperatorITBase
{
    @Override
    protected void setupCreateSchema()
    {
        t = createTable(
            "schema", "t",
            "id int not null primary key",
            "a int",
            "b int",
            "c int");
        createUniqueIndex("schema", "t", "idx_abc", "a", "b", "c");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        tRowType = schema.tableRowType(table(t));
        idxRowType = indexType(t, "a", "b", "c");
        db = new Row[]{
            // No nulls
            row(t, 1000L, 1L, 11L, 111L),
            row(t, 1001L, 1L, 11L, 112L),
            row(t, 1002L, 1L, 12L, 121L),
            row(t, 1003L, 1L, 12L, 122L),
            row(t, 1004L, 2L, 21L, 211L),
            row(t, 1005L, 2L, 21L, 212L),
            row(t, 1006L, 2L, 22L, 221L),
            row(t, 1007L, 2L, 22L, 222L),
            // With nulls
            row(t, 2000L, 3L, 4L, 5L),
            row(t, 2001L, 3L, 4L, null),
            row(t, 2002L, 3L, null, 5L),
            row(t, 2003L, 3L, null, null),
            row(t, 2004L, null, 4L, 5L),
            row(t, 2005L, null, 4L, null),
            row(t, 2006L, null, null, 5L),
            row(t, 2007L, null, null, null),
            // Duplicates of rows with nulls, which should really be kept in a unique index.
            row(t, 3001L, 3L, 4L, null),
            row(t, 3002L, 3L, null, 5L),
            row(t, 3003L, 3L, null, null),
            row(t, 3004L, null, 4L, 5L),
            row(t, 3005L, null, 4L, null),
            row(t, 3006L, null, null, 5L),
            row(t, 3007L, null, null, null),
        };
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
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
        Row[] expected = new Row[]{
            row(idxRowType, null, null, null, 2007L),
            row(idxRowType, null, null, null, 3007L),
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, null, null, 5L, 3006L),
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, null, 4L, null, 3005L),
            row(idxRowType, null, 4L, 5L, 2004L),
            row(idxRowType, null, 4L, 5L, 3004L),
            row(idxRowType, 1L, 11L, 111L, 1000L),
            row(idxRowType, 1L, 11L, 112L, 1001L),
            row(idxRowType, 1L, 12L, 121L, 1002L),
            row(idxRowType, 1L, 12L, 122L, 1003L),
            row(idxRowType, 2L, 21L, 211L, 1004L),
            row(idxRowType, 2L, 21L, 212L, 1005L),
            row(idxRowType, 2L, 22L, 221L, 1006L),
            row(idxRowType, 2L, 22L, 222L, 1007L),
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 3L, null, null, 3003L),
            row(idxRowType, 3L, null, 5L, 2002L),
            row(idxRowType, 3L, null, 5L, 3002L),
            row(idxRowType, 3L, 4L, null, 2001L),
            row(idxRowType, 3L, 4L, null, 3001L),
            row(idxRowType, 3L, 4L, 5L, 2000L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testAscSome_ABC()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, ASC, B, ASC, C, ASC));
        Row[] expected = new Row[]{
            row(idxRowType, null, null, null, 2007L),
            row(idxRowType, null, null, null, 3007L),
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, null, null, 5L, 3006L),
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, null, 4L, null, 3005L),
            row(idxRowType, null, 4L, 5L, 2004L),
            row(idxRowType, null, 4L, 5L, 3004L),
            row(idxRowType, 1L, 11L, 111L, 1000L),
            row(idxRowType, 1L, 11L, 112L, 1001L),
            row(idxRowType, 1L, 12L, 121L, 1002L),
            row(idxRowType, 1L, 12L, 122L, 1003L),
            row(idxRowType, 2L, 21L, 211L, 1004L),
            row(idxRowType, 2L, 21L, 212L, 1005L),
            row(idxRowType, 2L, 22L, 221L, 1006L),
            row(idxRowType, 2L, 22L, 222L, 1007L),
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 3L, null, null, 3003L),
            row(idxRowType, 3L, null, 5L, 2002L),
            row(idxRowType, 3L, null, 5L, 3002L),
            row(idxRowType, 3L, 4L, null, 2001L),
            row(idxRowType, 3L, 4L, null, 3001L),
            row(idxRowType, 3L, 4L, 5L, 2000L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testAscSome_AB()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, ASC, B, ASC));
        Row[] expected = new Row[]{
            row(idxRowType, null, null, null, 2007L),
            row(idxRowType, null, null, null, 3007L),
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, null, null, 5L, 3006L),
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, null, 4L, null, 3005L),
            row(idxRowType, null, 4L, 5L, 2004L),
            row(idxRowType, null, 4L, 5L, 3004L),
            row(idxRowType, 1L, 11L, 111L, 1000L),
            row(idxRowType, 1L, 11L, 112L, 1001L),
            row(idxRowType, 1L, 12L, 121L, 1002L),
            row(idxRowType, 1L, 12L, 122L, 1003L),
            row(idxRowType, 2L, 21L, 211L, 1004L),
            row(idxRowType, 2L, 21L, 212L, 1005L),
            row(idxRowType, 2L, 22L, 221L, 1006L),
            row(idxRowType, 2L, 22L, 222L, 1007L),
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 3L, null, null, 3003L),
            row(idxRowType, 3L, null, 5L, 2002L),
            row(idxRowType, 3L, null, 5L, 3002L),
            row(idxRowType, 3L, 4L, null, 2001L),
            row(idxRowType, 3L, 4L, null, 3001L),
            row(idxRowType, 3L, 4L, 5L, 2000L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testAscSome_A()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, ASC));
        Row[] expected = new Row[]{
            row(idxRowType, null, null, null, 2007L),
            row(idxRowType, null, null, null, 3007L),
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, null, null, 5L, 3006L),
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, null, 4L, null, 3005L),
            row(idxRowType, null, 4L, 5L, 2004L),
            row(idxRowType, null, 4L, 5L, 3004L),
            row(idxRowType, 1L, 11L, 111L, 1000L),
            row(idxRowType, 1L, 11L, 112L, 1001L),
            row(idxRowType, 1L, 12L, 121L, 1002L),
            row(idxRowType, 1L, 12L, 122L, 1003L),
            row(idxRowType, 2L, 21L, 211L, 1004L),
            row(idxRowType, 2L, 21L, 212L, 1005L),
            row(idxRowType, 2L, 22L, 221L, 1006L),
            row(idxRowType, 2L, 22L, 222L, 1007L),
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 3L, null, null, 3003L),
            row(idxRowType, 3L, null, 5L, 2002L),
            row(idxRowType, 3L, null, 5L, 3002L),
            row(idxRowType, 3L, 4L, null, 2001L),
            row(idxRowType, 3L, 4L, null, 3001L),
            row(idxRowType, 3L, 4L, 5L, 2000L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testDescAll()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, DESC, B, DESC, C, DESC, ID, DESC));
        Row[] expected = new Row[]{
            row(idxRowType, 3L, 4L, 5L, 2000L),
            row(idxRowType, 3L, 4L, null, 3001L),
            row(idxRowType, 3L, 4L, null, 2001L),
            row(idxRowType, 3L, null, 5L, 3002L),
            row(idxRowType, 3L, null, 5L, 2002L),
            row(idxRowType, 3L, null, null, 3003L),
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 2L, 22L, 222L, 1007L),
            row(idxRowType, 2L, 22L, 221L, 1006L),
            row(idxRowType, 2L, 21L, 212L, 1005L),
            row(idxRowType, 2L, 21L, 211L, 1004L),
            row(idxRowType, 1L, 12L, 122L, 1003L),
            row(idxRowType, 1L, 12L, 121L, 1002L),
            row(idxRowType, 1L, 11L, 112L, 1001L),
            row(idxRowType, 1L, 11L, 111L, 1000L),
            row(idxRowType, null, 4L, 5L, 3004L),
            row(idxRowType, null, 4L, 5L, 2004L),
            row(idxRowType, null, 4L, null, 3005L),
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, null, null, 5L, 3006L),
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, null, null, null, 3007L),
            row(idxRowType, null, null, null, 2007L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testDescSome_ABC()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, DESC, B, DESC, C, DESC));
        Row[] expected = new Row[]{
            row(idxRowType, 3L, 4L, 5L, 2000L),
            row(idxRowType, 3L, 4L, null, 3001L),
            row(idxRowType, 3L, 4L, null, 2001L),
            row(idxRowType, 3L, null, 5L, 3002L),
            row(idxRowType, 3L, null, 5L, 2002L),
            row(idxRowType, 3L, null, null, 3003L),
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 2L, 22L, 222L, 1007L),
            row(idxRowType, 2L, 22L, 221L, 1006L),
            row(idxRowType, 2L, 21L, 212L, 1005L),
            row(idxRowType, 2L, 21L, 211L, 1004L),
            row(idxRowType, 1L, 12L, 122L, 1003L),
            row(idxRowType, 1L, 12L, 121L, 1002L),
            row(idxRowType, 1L, 11L, 112L, 1001L),
            row(idxRowType, 1L, 11L, 111L, 1000L),
            row(idxRowType, null, 4L, 5L, 3004L),
            row(idxRowType, null, 4L, 5L, 2004L),
            row(idxRowType, null, 4L, null, 3005L),
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, null, null, 5L, 3006L),
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, null, null, null, 3007L),
            row(idxRowType, null, null, null, 2007L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testDescSome_AB()
    {
        // All specified orderings are DESC, so a unidirectional traversal is done. That's why everything
        // is in reverse order, not just the columns listed explicitly in the ordering.
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, DESC, B, DESC));
        Row[] expected = new Row[]{
            row(idxRowType, 3L, 4L, 5L, 2000L),
            row(idxRowType, 3L, 4L, null, 3001L),
            row(idxRowType, 3L, 4L, null, 2001L),
            row(idxRowType, 3L, null, 5L, 3002L),
            row(idxRowType, 3L, null, 5L, 2002L),
            row(idxRowType, 3L, null, null, 3003L),
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 2L, 22L, 222L, 1007L),
            row(idxRowType, 2L, 22L, 221L, 1006L),
            row(idxRowType, 2L, 21L, 212L, 1005L),
            row(idxRowType, 2L, 21L, 211L, 1004L),
            row(idxRowType, 1L, 12L, 122L, 1003L),
            row(idxRowType, 1L, 12L, 121L, 1002L),
            row(idxRowType, 1L, 11L, 112L, 1001L),
            row(idxRowType, 1L, 11L, 111L, 1000L),
            row(idxRowType, null, 4L, 5L, 3004L),
            row(idxRowType, null, 4L, 5L, 2004L),
            row(idxRowType, null, 4L, null, 3005L),
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, null, null, 5L, 3006L),
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, null, null, null, 3007L),
            row(idxRowType, null, null, null, 2007L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testDescSome_A()
    {
        // All specified orderings are DESC, so a unidirectional traversal is done. That's why everything
        // is in reverse order, not just the columns listed explicitly in the ordering.
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, DESC));
        Row[] expected = new Row[]{
            row(idxRowType, 3L, 4L, 5L, 2000L),
            row(idxRowType, 3L, 4L, null, 3001L),
            row(idxRowType, 3L, 4L, null, 2001L),
            row(idxRowType, 3L, null, 5L, 3002L),
            row(idxRowType, 3L, null, 5L, 2002L),
            row(idxRowType, 3L, null, null, 3003L),
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 2L, 22L, 222L, 1007L),
            row(idxRowType, 2L, 22L, 221L, 1006L),
            row(idxRowType, 2L, 21L, 212L, 1005L),
            row(idxRowType, 2L, 21L, 211L, 1004L),
            row(idxRowType, 1L, 12L, 122L, 1003L),
            row(idxRowType, 1L, 12L, 121L, 1002L),
            row(idxRowType, 1L, 11L, 112L, 1001L),
            row(idxRowType, 1L, 11L, 111L, 1000L),
            row(idxRowType, null, 4L, 5L, 3004L),
            row(idxRowType, null, 4L, 5L, 2004L),
            row(idxRowType, null, 4L, null, 3005L),
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, null, null, 5L, 3006L),
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, null, null, null, 3007L),
            row(idxRowType, null, null, null, 2007L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testMixedAll_AADA()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, ASC, B, ASC, C, DESC, ID, ASC));
        Row[] expected = new Row[]{
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, null, null, 5L, 3006L),
            row(idxRowType, null, null, null, 2007L),
            row(idxRowType, null, null, null, 3007L),
            row(idxRowType, null, 4L, 5L, 2004L),
            row(idxRowType, null, 4L, 5L, 3004L),
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, null, 4L, null, 3005L),
            row(idxRowType, 1L, 11L, 112L, 1001L),
            row(idxRowType, 1L, 11L, 111L, 1000L),
            row(idxRowType, 1L, 12L, 122L, 1003L),
            row(idxRowType, 1L, 12L, 121L, 1002L),
            row(idxRowType, 2L, 21L, 212L, 1005L),
            row(idxRowType, 2L, 21L, 211L, 1004L),
            row(idxRowType, 2L, 22L, 222L, 1007L),
            row(idxRowType, 2L, 22L, 221L, 1006L),
            row(idxRowType, 3L, null, 5L, 2002L),
            row(idxRowType, 3L, null, 5L, 3002L),
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 3L, null, null, 3003L),
            row(idxRowType, 3L, 4L, 5L, 2000L),
            row(idxRowType, 3L, 4L, null, 2001L),
            row(idxRowType, 3L, 4L, null, 3001L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testMixedSome_AAD()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, ASC, B, ASC, C, DESC));
        Row[] expected = new Row[]{
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, null, null, 5L, 3006L),
            row(idxRowType, null, null, null, 2007L),
            row(idxRowType, null, null, null, 3007L),
            row(idxRowType, null, 4L, 5L, 2004L),
            row(idxRowType, null, 4L, 5L, 3004L),
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, null, 4L, null, 3005L),
            row(idxRowType, 1L, 11L, 112L, 1001L),
            row(idxRowType, 1L, 11L, 111L, 1000L),
            row(idxRowType, 1L, 12L, 122L, 1003L),
            row(idxRowType, 1L, 12L, 121L, 1002L),
            row(idxRowType, 2L, 21L, 212L, 1005L),
            row(idxRowType, 2L, 21L, 211L, 1004L),
            row(idxRowType, 2L, 22L, 222L, 1007L),
            row(idxRowType, 2L, 22L, 221L, 1006L),
            row(idxRowType, 3L, null, 5L, 2002L),
            row(idxRowType, 3L, null, 5L, 3002L),
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 3L, null, null, 3003L),
            row(idxRowType, 3L, 4L, 5L, 2000L),
            row(idxRowType, 3L, 4L, null, 2001L),
            row(idxRowType, 3L, 4L, null, 3001L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testMixedAll_ADAA()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, ASC, B, DESC, C, ASC, ID, ASC));
        Row[] expected = new Row[]{
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, null, 4L, null, 3005L),
            row(idxRowType, null, 4L, 5L, 2004L),
            row(idxRowType, null, 4L, 5L, 3004L),
            row(idxRowType, null, null, null, 2007L),
            row(idxRowType, null, null, null, 3007L),
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, null, null, 5L, 3006L),
            row(idxRowType, 1L, 12L, 121L, 1002L),
            row(idxRowType, 1L, 12L, 122L, 1003L),
            row(idxRowType, 1L, 11L, 111L, 1000L),
            row(idxRowType, 1L, 11L, 112L, 1001L),
            row(idxRowType, 2L, 22L, 221L, 1006L),
            row(idxRowType, 2L, 22L, 222L, 1007L),
            row(idxRowType, 2L, 21L, 211L, 1004L),
            row(idxRowType, 2L, 21L, 212L, 1005L),
            row(idxRowType, 3L, 4L, null, 2001L),
            row(idxRowType, 3L, 4L, null, 3001L),
            row(idxRowType, 3L, 4L, 5L, 2000L),
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 3L, null, null, 3003L),
            row(idxRowType, 3L, null, 5L, 2002L),
            row(idxRowType, 3L, null, 5L, 3002L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testMixedSome_ADA()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, ASC, B, DESC, C, ASC));
        Row[] expected = new Row[]{
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, null, 4L, null, 3005L),
            row(idxRowType, null, 4L, 5L, 2004L),
            row(idxRowType, null, 4L, 5L, 3004L),
            row(idxRowType, null, null, null, 2007L),
            row(idxRowType, null, null, null, 3007L),
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, null, null, 5L, 3006L),
            row(idxRowType, 1L, 12L, 121L, 1002L),
            row(idxRowType, 1L, 12L, 122L, 1003L),
            row(idxRowType, 1L, 11L, 111L, 1000L),
            row(idxRowType, 1L, 11L, 112L, 1001L),
            row(idxRowType, 2L, 22L, 221L, 1006L),
            row(idxRowType, 2L, 22L, 222L, 1007L),
            row(idxRowType, 2L, 21L, 211L, 1004L),
            row(idxRowType, 2L, 21L, 212L, 1005L),
            row(idxRowType, 3L, 4L, null, 2001L),
            row(idxRowType, 3L, 4L, null, 3001L),
            row(idxRowType, 3L, 4L, 5L, 2000L),
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 3L, null, null, 3003L),
            row(idxRowType, 3L, null, 5L, 2002L),
            row(idxRowType, 3L, null, 5L, 3002L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testMixedSome_AD()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, ASC, B, DESC));
        Row[] expected = new Row[]{
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, null, 4L, null, 3005L),
            row(idxRowType, null, 4L, 5L, 2004L),
            row(idxRowType, null, 4L, 5L, 3004L),
            row(idxRowType, null, null, null, 2007L),
            row(idxRowType, null, null, null, 3007L),
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, null, null, 5L, 3006L),
            row(idxRowType, 1L, 12L, 121L, 1002L),
            row(idxRowType, 1L, 12L, 122L, 1003L),
            row(idxRowType, 1L, 11L, 111L, 1000L),
            row(idxRowType, 1L, 11L, 112L, 1001L),
            row(idxRowType, 2L, 22L, 221L, 1006L),
            row(idxRowType, 2L, 22L, 222L, 1007L),
            row(idxRowType, 2L, 21L, 211L, 1004L),
            row(idxRowType, 2L, 21L, 212L, 1005L),
            row(idxRowType, 3L, 4L, null, 2001L),
            row(idxRowType, 3L, 4L, null, 3001L),
            row(idxRowType, 3L, 4L, 5L, 2000L),
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 3L, null, null, 3003L),
            row(idxRowType, 3L, null, 5L, 2002L),
            row(idxRowType, 3L, null, 5L, 3002L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testMixedAll_ADDA()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, ASC, B, DESC, C, DESC, ID, ASC));
        Row[] expected = new Row[]{
            row(idxRowType, null, 4L, 5L, 2004L),
            row(idxRowType, null, 4L, 5L, 3004L),
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, null, 4L, null, 3005L),
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, null, null, 5L, 3006L),
            row(idxRowType, null, null, null, 2007L),
            row(idxRowType, null, null, null, 3007L),
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
            row(idxRowType, 3L, 4L, null, 3001L),
            row(idxRowType, 3L, null, 5L, 2002L),
            row(idxRowType, 3L, null, 5L, 3002L),
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 3L, null, null, 3003L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testMixedSome_ADD()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, ASC, B, DESC, C, DESC));
        Row[] expected = new Row[]{
            row(idxRowType, null, 4L, 5L, 2004L),
            row(idxRowType, null, 4L, 5L, 3004L),
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, null, 4L, null, 3005L),
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, null, null, 5L, 3006L),
            row(idxRowType, null, null, null, 2007L),
            row(idxRowType, null, null, null, 3007L),
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
            row(idxRowType, 3L, 4L, null, 3001L),
            row(idxRowType, 3L, null, 5L, 2002L),
            row(idxRowType, 3L, null, 5L, 3002L),
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 3L, null, null, 3003L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testMixedAll_DAAA()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, DESC, B, ASC, C, ASC, ID, ASC));
        Row[] expected = new Row[]{
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 3L, null, null, 3003L),
            row(idxRowType, 3L, null, 5L, 2002L),
            row(idxRowType, 3L, null, 5L, 3002L),
            row(idxRowType, 3L, 4L, null, 2001L),
            row(idxRowType, 3L, 4L, null, 3001L),
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
            row(idxRowType, null, null, null, 3007L),
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, null, null, 5L, 3006L),
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, null, 4L, null, 3005L),
            row(idxRowType, null, 4L, 5L, 2004L),
            row(idxRowType, null, 4L, 5L, 3004L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testMixedSome_DAA()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, DESC, B, ASC, C, ASC));
        Row[] expected = new Row[]{
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 3L, null, null, 3003L),
            row(idxRowType, 3L, null, 5L, 2002L),
            row(idxRowType, 3L, null, 5L, 3002L),
            row(idxRowType, 3L, 4L, null, 2001L),
            row(idxRowType, 3L, 4L, null, 3001L),
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
            row(idxRowType, null, null, null, 3007L),
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, null, null, 5L, 3006L),
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, null, 4L, null, 3005L),
            row(idxRowType, null, 4L, 5L, 2004L),
            row(idxRowType, null, 4L, 5L, 3004L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testMixedSome_DA()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, DESC, B, ASC));
        Row[] expected = new Row[]{
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 3L, null, null, 3003L),
            row(idxRowType, 3L, null, 5L, 2002L),
            row(idxRowType, 3L, null, 5L, 3002L),
            row(idxRowType, 3L, 4L, null, 2001L),
            row(idxRowType, 3L, 4L, null, 3001L),
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
            row(idxRowType, null, null, null, 3007L),
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, null, null, 5L, 3006L),
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, null, 4L, null, 3005L),
            row(idxRowType, null, 4L, 5L, 2004L),
            row(idxRowType, null, 4L, 5L, 3004L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testMixedAll_DADA()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, DESC, B, ASC, C, DESC, ID, ASC));
        Row[] expected = new Row[]{
            row(idxRowType, 3L, null, 5L, 2002L),
            row(idxRowType, 3L, null, 5L, 3002L),
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 3L, null, null, 3003L),
            row(idxRowType, 3L, 4L, 5L, 2000L),
            row(idxRowType, 3L, 4L, null, 2001L),
            row(idxRowType, 3L, 4L, null, 3001L),
            row(idxRowType, 2L, 21L, 212L, 1005L),
            row(idxRowType, 2L, 21L, 211L, 1004L),
            row(idxRowType, 2L, 22L, 222L, 1007L),
            row(idxRowType, 2L, 22L, 221L, 1006L),
            row(idxRowType, 1L, 11L, 112L, 1001L),
            row(idxRowType, 1L, 11L, 111L, 1000L),
            row(idxRowType, 1L, 12L, 122L, 1003L),
            row(idxRowType, 1L, 12L, 121L, 1002L),
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, null, null, 5L, 3006L),
            row(idxRowType, null, null, null, 2007L),
            row(idxRowType, null, null, null, 3007L),
            row(idxRowType, null, 4L, 5L, 2004L),
            row(idxRowType, null, 4L, 5L, 3004L),
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, null, 4L, null, 3005L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testMixedSome_DAD()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, DESC, B, ASC, C, DESC));
        Row[] expected = new Row[]{
            row(idxRowType, 3L, null, 5L, 2002L),
            row(idxRowType, 3L, null, 5L, 3002L),
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 3L, null, null, 3003L),
            row(idxRowType, 3L, 4L, 5L, 2000L),
            row(idxRowType, 3L, 4L, null, 2001L),
            row(idxRowType, 3L, 4L, null, 3001L),
            row(idxRowType, 2L, 21L, 212L, 1005L),
            row(idxRowType, 2L, 21L, 211L, 1004L),
            row(idxRowType, 2L, 22L, 222L, 1007L),
            row(idxRowType, 2L, 22L, 221L, 1006L),
            row(idxRowType, 1L, 11L, 112L, 1001L),
            row(idxRowType, 1L, 11L, 111L, 1000L),
            row(idxRowType, 1L, 12L, 122L, 1003L),
            row(idxRowType, 1L, 12L, 121L, 1002L),
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, null, null, 5L, 3006L),
            row(idxRowType, null, null, null, 2007L),
            row(idxRowType, null, null, null, 3007L),
            row(idxRowType, null, 4L, 5L, 2004L),
            row(idxRowType, null, 4L, 5L, 3004L),
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, null, 4L, null, 3005L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testMixedAll_DDAA()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, DESC, B, DESC, C, ASC, ID, ASC));
        Row[] expected = new Row[]{
            row(idxRowType, 3L, 4L, null, 2001L),
            row(idxRowType, 3L, 4L, null, 3001L),
            row(idxRowType, 3L, 4L, 5L, 2000L),
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 3L, null, null, 3003L),
            row(idxRowType, 3L, null, 5L, 2002L),
            row(idxRowType, 3L, null, 5L, 3002L),
            row(idxRowType, 2L, 22L, 221L, 1006L),
            row(idxRowType, 2L, 22L, 222L, 1007L),
            row(idxRowType, 2L, 21L, 211L, 1004L),
            row(idxRowType, 2L, 21L, 212L, 1005L),
            row(idxRowType, 1L, 12L, 121L, 1002L),
            row(idxRowType, 1L, 12L, 122L, 1003L),
            row(idxRowType, 1L, 11L, 111L, 1000L),
            row(idxRowType, 1L, 11L, 112L, 1001L),
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, null, 4L, null, 3005L),
            row(idxRowType, null, 4L, 5L, 2004L),
            row(idxRowType, null, 4L, 5L, 3004L),
            row(idxRowType, null, null, null, 2007L),
            row(idxRowType, null, null, null, 3007L),
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, null, null, 5L, 3006L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testMixedSome_DDA()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, DESC, B, DESC, C, ASC));
        Row[] expected = new Row[]{
            row(idxRowType, 3L, 4L, null, 2001L),
            row(idxRowType, 3L, 4L, null, 3001L),
            row(idxRowType, 3L, 4L, 5L, 2000L),
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 3L, null, null, 3003L),
            row(idxRowType, 3L, null, 5L, 2002L),
            row(idxRowType, 3L, null, 5L, 3002L),
            row(idxRowType, 2L, 22L, 221L, 1006L),
            row(idxRowType, 2L, 22L, 222L, 1007L),
            row(idxRowType, 2L, 21L, 211L, 1004L),
            row(idxRowType, 2L, 21L, 212L, 1005L),
            row(idxRowType, 1L, 12L, 121L, 1002L),
            row(idxRowType, 1L, 12L, 122L, 1003L),
            row(idxRowType, 1L, 11L, 111L, 1000L),
            row(idxRowType, 1L, 11L, 112L, 1001L),
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, null, 4L, null, 3005L),
            row(idxRowType, null, 4L, 5L, 2004L),
            row(idxRowType, null, 4L, 5L, 3004L),
            row(idxRowType, null, null, null, 2007L),
            row(idxRowType, null, null, null, 3007L),
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, null, null, 5L, 3006L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testMixedAll_DDDA()
    {
        Operator plan = indexScan_Default(idxRowType, unbounded(), ordering(A, DESC, B, DESC, C, DESC, ID, ASC));
        Row[] expected = new Row[]{
            row(idxRowType, 3L, 4L, 5L, 2000L),
            row(idxRowType, 3L, 4L, null, 2001L),
            row(idxRowType, 3L, 4L, null, 3001L),
            row(idxRowType, 3L, null, 5L, 2002L),
            row(idxRowType, 3L, null, 5L, 3002L),
            row(idxRowType, 3L, null, null, 2003L),
            row(idxRowType, 3L, null, null, 3003L),
            row(idxRowType, 2L, 22L, 222L, 1007L),
            row(idxRowType, 2L, 22L, 221L, 1006L),
            row(idxRowType, 2L, 21L, 212L, 1005L),
            row(idxRowType, 2L, 21L, 211L, 1004L),
            row(idxRowType, 1L, 12L, 122L, 1003L),
            row(idxRowType, 1L, 12L, 121L, 1002L),
            row(idxRowType, 1L, 11L, 112L, 1001L),
            row(idxRowType, 1L, 11L, 111L, 1000L),
            row(idxRowType, null, 4L, 5L, 2004L),
            row(idxRowType, null, 4L, 5L, 3004L),
            row(idxRowType, null, 4L, null, 2005L),
            row(idxRowType, null, 4L, null, 3005L),
            row(idxRowType, null, null, 5L, 2006L),
            row(idxRowType, null, null, 5L, 3006L),
            row(idxRowType, null, null, null, 2007L),
            row(idxRowType, null, null, null, 3007L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
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
            ordering.append(field(idxRowType, column), asc);
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
