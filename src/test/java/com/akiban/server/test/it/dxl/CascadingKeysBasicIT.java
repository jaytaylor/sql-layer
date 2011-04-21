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

package com.akiban.server.test.it.dxl;

import com.akiban.server.IndexDef;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.test.it.ITBase;
import com.akiban.server.test.it.keyupdate.RecordCollectingIndexRecordVisistor;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public final class CascadingKeysBasicIT extends ITBase {
    private int customers;
    private int orders;
    private int items;

    @Before
    public void setUp() throws Exception {
        String schema = "cascading";
        customers = createTable(schema, "customers", "cid int key");
        orders = createTable(schema, "orders",
                "cid int",
                "oid int",
                "PRIMARY KEY(cid,oid)",
                "CONSTRAINT __akiban_o FOREIGN KEY __akiban_o(cid) REFERENCES customers(cid)"
        );
        items = createTable(schema, "items",
                "cid int",
                "oid int",
                "iid int",
                "PRIMARY KEY (cid,oid,iid)",
                "CONSTRAINT __akiban_i FOREIGN KEY __akiban_i(cid,oid) REFERENCES orders(cid,oid)"
        );

        writeRows(
                createNewRow(customers, 1),
                createNewRow(orders, 1, 1),
                createNewRow(items, 1, 1, 1),
                createNewRow(items, 1, 1, 2),
                createNewRow(orders, 2, 2),
                createNewRow(items, 2, 2, 1)

        );
    }

    @Test
    public void traverseCustomersPK() throws Exception {
        traversePK(
                customers,
                Arrays.asList(1L)
        );
    }

    @Test
    public void traverseOrdersPK() throws Exception {
        traversePK(
                orders,
                Arrays.asList(1L, 1L),
                Arrays.asList(2L, 2L)
        );
    }

    @Test
    public void traverseItemsPK() throws Exception {
        traversePK(
                items,
                Arrays.asList(1L, 1L, 1L),
                Arrays.asList(1L, 1L, 2L),
                Arrays.asList(2L, 2L, 2L)
        );
    }

    private void traversePK(int rowDefId, List<? extends Long>... expectedIndexes) throws Exception {
        IndexDef pkIndexDef = rowDefCache().rowDef(rowDefId).getPKIndexDef();

        RecordCollectingIndexRecordVisistor visitor = new RecordCollectingIndexRecordVisistor();
        persistitStore().traverse(session(), pkIndexDef, visitor);

        assertEquals("traversed indexes", Arrays.asList(expectedIndexes), visitor.records());
    }

    @Test
    public void scanCustomers() throws InvalidOperationException {

        List<NewRow> actual = scanAll(scanAllRequest(customers));
        List<NewRow> expected = Arrays.asList(
                createNewRow(customers, 1L)
        );
        assertEquals("rows scanned", expected, actual);
    }

    @Test
    public void scanOrders() throws InvalidOperationException {

        List<NewRow> actual = scanAll(scanAllRequest(orders));
        List<NewRow> expected = Arrays.asList(
                createNewRow(orders, 1L, 1L),
                createNewRow(orders, 2L, 2L)
        );
        assertEquals("rows scanned", expected, actual);
    }

    @Test
    public void scanItems() throws InvalidOperationException {
        List<NewRow> actual = scanAll(scanAllRequest(items));
        List<NewRow> expected = Arrays.asList(
                createNewRow(items, 1L, 1L, 1L),
                createNewRow(items, 1L, 1L, 2L),
                createNewRow(items, 2L, 2L, 1L)
        );
        assertEquals("rows scanned", expected, actual);
    }
}
