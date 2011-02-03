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

package com.akiban.cserver.itests.keyupdate;

import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.itests.ApiTestBase;
import com.persistit.exception.PersistitException;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static com.akiban.cserver.itests.keyupdate.Schema.*;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class KeyUpdateIT extends ApiTestBase
{
    @Before
    public void before() throws Exception
    {
        testStore = new TestStore(persistitStore());
        createSchema();
        populateTables();
    }

    @Test
    public void testInitialState() throws Exception
    {
        RecordCollectingRecordVisistor testVisitor = new RecordCollectingRecordVisistor();
        RecordCollectingRecordVisistor realVisitor = new RecordCollectingRecordVisistor();
        testStore.traverse(session, groupRowDef, testVisitor, realVisitor);
        assertEquals(testVisitor.records(), realVisitor.records());
    }

/*
    @Test
    public void testLeafFKUpdate() throws Exception
    {
        UpdatedRecordVisitor updatedRecordVisitor = new UpdatedRecordVisitor(new InitialStateRecordVisistor());
        TestRow oldRow = testStore.find(new HKey(customerRowDef, 2L, orderRowDef, 22L, itemRowDef, 222L));
        TestRow newRow = copyRow(oldRow);
        newRow.put(i_oid, 0);
        update(oldRow, newRow);
        KeyCollectingRecordVisistor visitor = new KeyCollectingRecordVisistor(updatedRecordVisitor);
        persistitStore().traverse(session, groupRowDef, visitor);
        assertEquals(testStore.hKeys(), visitor.hKeys());
    }
*/

    private void createSchema() throws InvalidOperationException
    {
        // customer
        customerId = createTable("coi", "customer",
                                 "cid int not null key",
                                 "cx int");
        c_cid = 0;
        c_cx = 1;
        // order
        orderId = createTable("coi", "order",
                              "oid int not null key",
                              "cid int",
                              "ox int",
                              "constraint __akiban_oc foreign key __akiban_oc(cid) references customer(cid)");
        o_oid = 0;
        o_cid = 1;
        o_ox = 2;
        // item
        itemId = createTable("coi", "item",
                             "iid int not null key",
                             "oid int",
                             "ix int",
                             "constraint __akiban_io foreign key __akiban_io(oid) references order(oid)");
        i_iid = 0;
        i_oid = 1;
        i_ix = 2;
        orderRowDef = rowDefCache().getRowDef(orderId);
        customerRowDef = rowDefCache().getRowDef(customerId);
        itemRowDef = rowDefCache().getRowDef(itemId);
        // group
        int groupRowDefId = customerRowDef.getGroupRowDefId();
        groupRowDef = store().getRowDefCache().getRowDef(groupRowDefId);
    }

    private void populateTables() throws Exception
    {
        TestRow order;
        insert(row(customerRowDef, 1, 100));
        insert(order = row(orderRowDef, 11, 1, 1100));
        insert(row(order, itemRowDef, 111, 11, 11100));
        insert(row(order, itemRowDef, 112, 11, 11200));
        insert(row(order, itemRowDef, 113, 11, 11300));
        insert(order = row(orderRowDef, 12, 1, 1200));
        insert(row(order, itemRowDef, 121, 12, 12100));
        insert(row(order, itemRowDef, 122, 12, 12200));
        insert(row(order, itemRowDef, 123, 12, 12300));
        insert(order = row(orderRowDef, 13, 1, 1300));
        insert(row(order, itemRowDef, 131, 13, 13100));
        insert(row(order, itemRowDef, 132, 13, 13200));
        insert(row(order, itemRowDef, 133, 13, 13300));
        insert(row(customerRowDef, 2, 200));
        insert(order = row(orderRowDef, 21, 2, 2100));
        insert(row(order, itemRowDef, 211, 21, 21100));
        insert(row(order, itemRowDef, 212, 21, 21200));
        insert(row(order, itemRowDef, 213, 21, 21300));
        insert(order = row(orderRowDef, 22, 2, 2200));
        insert(row(order, itemRowDef, 221, 22, 22100));
        insert(row(order, itemRowDef, 222, 22, 22200));
        insert(row(order, itemRowDef, 223, 22, 22300));
        insert(order = row(orderRowDef, 23, 2, 2300));
        insert(row(order, itemRowDef, 231, 23, 23100));
        insert(row(order, itemRowDef, 232, 23, 23200));
        insert(row(order, itemRowDef, 233, 23, 23300));
        insert(row(customerRowDef, 3, 300));
        insert(order = row(orderRowDef, 31, 3, 3100));
        insert(row(order, itemRowDef, 311, 31, 31100));
        insert(row(order, itemRowDef, 312, 31, 31200));
        insert(row(order, itemRowDef, 313, 31, 31300));
        insert(order = row(orderRowDef, 32, 3, 3200));
        insert(row(order, itemRowDef, 321, 32, 32100));
        insert(row(order, itemRowDef, 322, 32, 32200));
        insert(row(order, itemRowDef, 323, 32, 32300));
        insert(order = row(orderRowDef, 33, 3, 3300));
        insert(row(order, itemRowDef, 331, 33, 33100));
        insert(row(order, itemRowDef, 332, 33, 33200));
        insert(row(order, itemRowDef, 333, 33, 33300));
    }

    private TestRow row(RowDef table, Object... values)
    {
        TestRow row = new TestRow(table.getRowDefId());
        int column = 0;
        for (Object value : values) {
            if (value instanceof Integer) {
                value = ((Integer) value).longValue();
            }
            row.put(column++, value);
        }
        row.hKey(hKey(row));
        return row;
    }

    private TestRow row(TestRow parent, RowDef table, Object... values)
    {
        TestRow row = new TestRow(table.getRowDefId());
        int column = 0;
        for (Object value : values) {
            if (value instanceof Integer) {
                value = ((Integer) value).longValue();
            }
            row.put(column++, value);
        }
        row.hKey(hKey(row, parent));
        return row;
    }

    private void insert(TestRow row) throws Exception
    {
        testStore.writeRow(session, row);
    }

    private void update(TestRow oldRow, TestRow newRow) throws Exception
    {
        testStore.updateRow(session, oldRow, newRow, null);
    }

    private HKey hKey(TestRow row)
    {
        HKey hKey = null;
        RowDef rowDef = row.getRowDef();
        if (rowDef == customerRowDef) {
            hKey = new HKey(customerRowDef, row.get(c_cid));
        } else if (rowDef == orderRowDef) {
            hKey = new HKey(customerRowDef, row.get(o_cid),
                            orderRowDef, row.get(o_oid));
        } else {
            assertTrue(false);
        }
        return hKey;
    }

    private HKey hKey(TestRow row, TestRow parent)
    {
        HKey hKey = null;
        RowDef rowDef = row.getRowDef();
        if (rowDef == itemRowDef) {
            hKey = new HKey(customerRowDef, parent.get(o_cid),
                            orderRowDef, row.get(i_oid),
                            itemRowDef, row.get(i_iid));
        } else {
            assertTrue(false);
        }
        return hKey;
    }

    private TestRow copyRow(TestRow row)
    {
        TestRow copy = new TestRow(row.getTableId());
        for (Map.Entry<Integer, Object> entry : row.getFields().entrySet()) {
            copy.put(entry.getKey(), entry.getValue());
        }
        return copy;
    }

    private TestStore testStore;
}
