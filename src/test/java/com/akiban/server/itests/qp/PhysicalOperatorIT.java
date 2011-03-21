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

package com.akiban.server.itests.qp;

import com.akiban.ais.model.GroupTable;
import com.akiban.qp.*;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.persistitadapter.PersistitTableRow;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.RowDef;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.NiceRow;
import com.akiban.server.itests.ApiTestBase;
import com.akiban.server.store.PersistitStore;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PhysicalOperatorIT extends ApiTestBase
{
    @Before
    public void before() throws InvalidOperationException
    {
        customer = createTable(
            "schema", "customer",
            "cid int not null key");
        order = createTable(
            "schema", "order",
            "oid int not null key",
            "cid int",
            "constraint __akiban_oc foreign key __akiban_oc(cid) references customer(cid)");
        item = createTable(
            "schema", "item",
            "iid int not null key",
            "oid int",
            "constraint __akiban_io foreign key __akiban_io(oid) references order(oid)");
        db = new NewRow[]{createNewRow(customer, 1L),
                          createNewRow(customer, 2L),
                          createNewRow(order, 11L, 1L),
                          createNewRow(order, 12L, 1L),
                          createNewRow(order, 21L, 2L),
                          createNewRow(order, 22L, 2L),
                          createNewRow(item, 111L, 11L),
                          createNewRow(item, 112L, 11L),
                          createNewRow(item, 121L, 12L),
                          createNewRow(item, 122L, 12L),
                          createNewRow(item, 211L, 21L),
                          createNewRow(item, 212L, 21L),
                          createNewRow(item, 221L, 22L),
                          createNewRow(item, 222L, 22L)};
        writeRows(db);
    }

    @Test
    public void testGroupScan() throws Exception
    {
        PersistitAdapter adapter = new PersistitAdapter((PersistitStore) store(), session());
        GroupScan_Default groupScan = new GroupScan_Default(adapter, groupTable(customer));
        compare(db, groupScan);
    }

    @Test
    public void testSelect()
    {
        PersistitAdapter adapter = new PersistitAdapter((PersistitStore) store(), session());
        RowType customerRowType = adapter.rowType(rowDefCache().rowDef(customer));
        GroupScan_Default groupScan = new GroupScan_Default(adapter, groupTable(customer));
        Expression cidEq2 = new Compare(new Field(0), Comparison.EQ, new Literal(2L));
        Select_HKeyOrdered select = new Select_HKeyOrdered(groupScan, customerRowType, cidEq2);
        NewRow[] expected = new NewRow[]{createNewRow(customer, 2L),
                                         createNewRow(order, 21L, 2L),
                                         createNewRow(order, 22L, 2L),
                                         createNewRow(item, 211L, 21L),
                                         createNewRow(item, 212L, 21L),
                                         createNewRow(item, 221L, 22L),
                                         createNewRow(item, 222L, 22L)};
        compare(expected, select);
    }

    private GroupTable groupTable(int userTableId)
    {
        RowDef userTableRowDef = rowDefCache().rowDef(userTableId);
        return userTableRowDef.table().getGroup().getGroupTable();
    }

    private void compare(NewRow[] expected, PhysicalOperator plan)
    {
        List<NewRow> actual = new ArrayList<NewRow>();
        plan.open();
        Row row;
        while ((row = plan.next()) != null) {
            actual.add(((PersistitTableRow) row).rowWrapper().niceRow());
        }
        assertEquals(expected.length, actual.size());
        for (NewRow actualRow : actual) {
            assertTrue(actualRow instanceof NiceRow);
            boolean found = false;
            for (int i = 0; !found && i < expected.length; i++) {
                NewRow expectedRow = expected[i];
                assertTrue(expectedRow instanceof NiceRow);
                found = expectedRow.equals(actualRow);
            }
            assertTrue(found);
        }
    }

    private int customer;
    private int order;
    private int item;
    private NewRow[] db;
}
