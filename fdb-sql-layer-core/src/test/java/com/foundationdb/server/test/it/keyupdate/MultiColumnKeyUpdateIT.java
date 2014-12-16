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

package com.foundationdb.server.test.it.keyupdate;

import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.rowdata.RowDef;

import org.junit.Test;

import java.util.List;

import static com.foundationdb.server.test.it.keyupdate.Schema.*;
import static org.junit.Assert.*;

// Like KeyUpdateIT, but with multi-column keys

public class MultiColumnKeyUpdateIT extends KeyUpdateBase
{
    @Test
    public void testItemFKUpdate() throws Exception
    {
        // Set item.oid1 = 0 for item 1222
        KeyUpdateRow originalItem = testStore.find(new HKey(vendorRT, 1L, 1L,
                                                       customerRT, 12L, 12L,
                                                       orderRT, 122L, 122L,
                                                       itemRT, 1222L, 1222L));
        KeyUpdateRow updatedItem = updateRow(originalItem, i_oid1, 0L, null);
        startMonitoringHKeyPropagation();
        dbUpdate(originalItem, updatedItem);
        checkHKeyPropagation(0, 0);
        checkDB();
        // Revert change
        startMonitoringHKeyPropagation();
        dbUpdate(updatedItem, originalItem);
        checkHKeyPropagation(0, 0);
        checkDB();
        checkInitialState();
    }

    @Test
    public void testItemPKUpdate() throws Exception
    {
        // Set item.iid = 0 for item 1222
        KeyUpdateRow order = testStore.find(new HKey(vendorRT, 1L, 1L,
                                                customerRT, 12L, 12L,
                                                orderRT, 122L, 122L));
        KeyUpdateRow originalItem = testStore.find(new HKey(vendorRT, 1L, 1L,
                                                       customerRT, 12L, 12L,
                                                       orderRT, 122L, 122L,
                                                       itemRT, 1222L, 1222L));
        assertNotNull(order);
        KeyUpdateRow updatedItem = updateRow(originalItem, i_iid2, 0L, order);
        startMonitoringHKeyPropagation();
        dbUpdate(originalItem, updatedItem);
        checkHKeyPropagation(0, 0);
        checkDB();
        // Revert change
        startMonitoringHKeyPropagation();
        dbUpdate(updatedItem, originalItem);
        checkHKeyPropagation(0, 0);
        checkDB();
        checkInitialState();
    }

    @Test
    public void testItemPKUpdateCreatingDuplicate() throws Exception
    {
        // Set item.iid = 1223 for item 1222
        KeyUpdateRow order = testStore.find(new HKey(vendorRT, 1L, 1L,
                                                customerRT, 12L, 12L,
                                                orderRT, 122L, 122L));
        KeyUpdateRow originalItem = testStore.find(new HKey(vendorRT, 1L, 1L,
                                                       customerRT, 12L, 12L,
                                                       orderRT, 122L, 122L,
                                                       itemRT, 1222L, 1222L));
        assertNotNull(order);
        KeyUpdateRow updatedItem = updateRow(originalItem, i_iid1, 1223L, i_iid2, 1223L, order);
        try {
            dbUpdate(originalItem, updatedItem);
            fail();
        } catch (InvalidOperationException e) {
            assertEquals(ErrorCode.DUPLICATE_KEY, e.getCode());
        }
        checkDB();
        checkInitialState();
    }

    @Test
    public void testOrderFKUpdate() throws Exception
    {
        // Set order.cid = 0 for order 222
        KeyUpdateRow customer = testStore.find(new HKey(vendorRT, 2L, 2L,
                                                   customerRT, 22L, 22L));
        KeyUpdateRow originalOrder = testStore.find(new HKey(vendorRT, 2L, 2L,
                                                        customerRT, 22L, 22L,
                                                        orderRT, 222L, 222L));
        KeyUpdateRow updatedOrder = updateRow(originalOrder, o_cid1, 0L, o_cid2, 0L, null);
        // Propagate change to order 222's items to reflect db state
        for (long iid = 2221; iid <= 2223; iid++) {
            KeyUpdateRow oldItemRow = testStore.find(new HKey(vendorRT, 2L, 2L,
                                                         customerRT, 22L, 22L,
                                                         orderRT, 222L, 222L,
                                                         itemRT, iid, iid));
            KeyUpdateRow newItemRow = copyRow(oldItemRow);
            newItemRow.hKey(hKey(newItemRow, updatedOrder));
            testStore.deleteTestRow(oldItemRow);
            testStore.writeTestRow(newItemRow);
        }
        startMonitoringHKeyPropagation();
        dbUpdate(originalOrder, updatedOrder);
        checkHKeyPropagation(2, 6);
        checkDB();
        // Revert change
        for (long iid = 2221; iid <= 2223; iid++) {
            KeyUpdateRow oldItemRow = testStore.find(new HKey(vendorRT, null, null,
                                                         customerRT, 0L, 0L,
                                                         orderRT, 222L, 222L,
                                                         itemRT, iid, iid));
            KeyUpdateRow newItemRow = copyRow(oldItemRow);
            newItemRow.hKey(hKey(newItemRow, originalOrder, customer));
            testStore.deleteTestRow(oldItemRow);
            testStore.writeTestRow(newItemRow);
        }
        startMonitoringHKeyPropagation();
        dbUpdate(updatedOrder, originalOrder);
        checkHKeyPropagation(2, 6);
        checkDB();
        checkInitialState();
    }

    @Test
    public void testOrderPKUpdate() throws Exception
    {
        // Set order.oid = 0 for order 222
        KeyUpdateRow customer = testStore.find(new HKey(vendorRT, 2L, 2L,
                                                   customerRT, 22L, 22L));
        KeyUpdateRow originalOrder = testStore.find(new HKey(vendorRT, 2L, 2L,
                                                        customerRT, 22L, 22L,
                                                        orderRT, 222L, 222L));
        KeyUpdateRow udpatedOrder = updateRow(originalOrder, o_oid1, 0L, o_oid2, 0L, customer);
        // Propagate change to order 222's items to reflect db state
        for (long iid = 2221; iid <= 2223; iid++) {
            KeyUpdateRow oldItemRow = testStore.find(new HKey(vendorRT, 2L, 2L,
                                                         customerRT, 22L, 22L,
                                                         orderRT, 222L, 222L,
                                                         itemRT, iid, iid));
            KeyUpdateRow newItemRow = copyRow(oldItemRow);
            newItemRow.hKey(hKey(newItemRow, null));
            testStore.deleteTestRow(oldItemRow);
            testStore.writeTestRow(newItemRow);
        }
        startMonitoringHKeyPropagation();
        dbUpdate(originalOrder, udpatedOrder);
        checkHKeyPropagation(2, 3); // 3: 3 items become orphans, no items are adopted orphans.
        checkDB();
        // Revert change
        for (long iid = 2221; iid <= 2223; iid++) {
            KeyUpdateRow oldItemRow = testStore.find(new HKey(vendorRT, null, null,
                                                         customerRT,null, null,
                                                         orderRT, 222L, 222L,
                                                         itemRT, iid, iid));
            KeyUpdateRow newItemRow = copyRow(oldItemRow);
            newItemRow.hKey(hKey(newItemRow, originalOrder, customer));
            testStore.deleteTestRow(oldItemRow);
            testStore.writeTestRow(newItemRow);
        }
        startMonitoringHKeyPropagation();
        dbUpdate(udpatedOrder, originalOrder);
        checkHKeyPropagation(2, 3);
        checkDB();
        checkInitialState();
    }

    @Test
    public void testOrderPKUpdateCreatingDuplicate() throws Exception
    {
        // Set order.oid = 221 for order 222
        KeyUpdateRow customer = testStore.find(new HKey(vendorRT, 2L, 2L,
                                                   customerRT, 22L, 22L));
        KeyUpdateRow originalOrder = testStore.find(new HKey(vendorRT, 2L, 2L,
                                                        customerRT, 22L, 22L,
                                                        orderRT, 222L, 222L));
        assertNotNull(customer);
        KeyUpdateRow updatedOrder = updateRow(originalOrder, o_oid1, 221L, o_oid2, 221L, customer);
        try {
            dbUpdate(originalOrder, updatedOrder);
            fail();
        } catch (InvalidOperationException e) {
            assertEquals(ErrorCode.DUPLICATE_KEY, e.getCode());
        }
        checkDB();
    }

    @Test
    public void testCustomerFKUpdate() throws Exception
    {
        // Set customer.vid = 0 for customer 13
        KeyUpdateRow originalCustomer = testStore.find(new HKey(vendorRT, 1L, 1L,
                                                           customerRT, 13L, 13L));
        KeyUpdateRow udpatedCustomer = updateRow(originalCustomer, c_vid1, 0L, c_vid2, 0L, null);
        // Propagate change to customer 13s descendents to reflect db state
        for (long oid = 131; oid <= 133; oid++) {
            KeyUpdateRow oldOrderRow = testStore.find(new HKey(vendorRT, 1L, 1L,
                                                          customerRT, 13L, 13L,
                                                          orderRT, oid, oid));
            KeyUpdateRow newOrderRow = copyRow(oldOrderRow);
            newOrderRow.hKey(hKey(newOrderRow, udpatedCustomer));
            testStore.deleteTestRow(oldOrderRow);
            testStore.writeTestRow(newOrderRow);
            for (long iid = oid * 10 + 1; iid <= oid * 10 + 3; iid++) {
                KeyUpdateRow oldItemRow = testStore.find(new HKey(vendorRT, 1L, 1L,
                                                             customerRT, 13L, 13L,
                                                             orderRT, oid, oid,
                                                             itemRT, iid, iid));
                KeyUpdateRow newItemRow = copyRow(oldItemRow);
                newItemRow.hKey(hKey(newItemRow, newOrderRow, udpatedCustomer));
                testStore.deleteTestRow(oldItemRow);
                testStore.writeTestRow(newItemRow);
            }
        }
        startMonitoringHKeyPropagation();
        dbUpdate(originalCustomer, udpatedCustomer);
        checkHKeyPropagation(2, 24); // 3 orders, 3 items per order, delete/reinsert each
        checkDB();
        // Revert change
        for (long oid = 131; oid <= 133; oid++) {
            KeyUpdateRow oldOrderRow = testStore.find(new HKey(vendorRT, 0L, 0L,
                                                          customerRT, 13L, 13L,
                                                          orderRT, oid, oid));
            KeyUpdateRow newOrderRow = copyRow(oldOrderRow);
            newOrderRow.hKey(hKey(newOrderRow, originalCustomer));
            testStore.deleteTestRow(oldOrderRow);
            testStore.writeTestRow(newOrderRow);
            for (long iid = oid * 10 + 1; iid <= oid * 10 + 3; iid++) {
                KeyUpdateRow oldItemRow = testStore.find(new HKey(vendorRT, 0L, 0L,
                                                             customerRT, 13L, 13L,
                                                             orderRT, oid, oid,
                                                             itemRT, iid, iid));
                KeyUpdateRow newItemRow = copyRow(oldItemRow);
                newItemRow.hKey(hKey(newItemRow, newOrderRow, originalCustomer));
                testStore.deleteTestRow(oldItemRow);
                testStore.writeTestRow(newItemRow);
            }
        }
        startMonitoringHKeyPropagation();
        dbUpdate(udpatedCustomer, originalCustomer);
        checkHKeyPropagation(2, 24);
        checkDB();
        checkInitialState();
    }

    @Test
    public void testCustomerPKUpdate() throws Exception
    {
        // Set customer.cid = 0 for customer 22
        KeyUpdateRow originalCustomer = testStore.find(new HKey(vendorRT, 2L, 2L,
                                                           customerRT, 22L, 22L));
        
        KeyUpdateRow updatedCustomer = updateRow(originalCustomer, c_cid2, 23L, null);
        // Propagate change to customer 22's orders and items to reflect db state
        for (long oid = 221; oid <= 223; oid++) {
            KeyUpdateRow oldOrderRow = testStore.find(new HKey(vendorRT, 2L, 2L,
                                                          customerRT, 22L, 22L,
                                                          orderRT, oid, oid));
            KeyUpdateRow newOrderRow = copyRow(oldOrderRow);
            newOrderRow.hKey(hKey(newOrderRow, null));
            testStore.deleteTestRow(oldOrderRow);
            testStore.writeTestRow(newOrderRow);
            for (long iid = oid * 10 + 1; iid <= oid * 10 + 3; iid++) {
                KeyUpdateRow oldItemRow = testStore.find(new HKey(vendorRT, 2L, 2L,
                                                             customerRT, 22L, 22L,
                                                             orderRT, oid, oid,
                                                             itemRT, iid, iid));
                KeyUpdateRow newItemRow = copyRow(oldItemRow);
                newItemRow.hKey(hKey(newItemRow, newOrderRow));
                testStore.deleteTestRow(oldItemRow);
                testStore.writeTestRow(newItemRow);
            }
        }
        startMonitoringHKeyPropagation();
        dbUpdate(originalCustomer, updatedCustomer);
        checkHKeyPropagation(2, 12); // 3 orders, 3 items per order, delete, but no reinsert - there is no customer 0
        checkDB();
        // Revert change
        for (long oid = 221; oid <= 223; oid++) {
            KeyUpdateRow oldOrderRow = testStore.find(new HKey(vendorRT, null, null,
                                                          customerRT, 22L, 22L,
                                                          orderRT, oid, oid));
            KeyUpdateRow newOrderRow = copyRow(oldOrderRow);
            newOrderRow.hKey(hKey(newOrderRow, originalCustomer));
            testStore.deleteTestRow(oldOrderRow);
            testStore.writeTestRow(newOrderRow);
            for (long iid = oid * 10 + 1; iid <= oid * 10 + 3; iid++) {
                KeyUpdateRow oldItemRow = testStore.find(new HKey(vendorRT, null, null,
                                                             customerRT, 22L, 22L,
                                                             orderRT, oid, oid,
                                                             itemRT, iid, iid));
                KeyUpdateRow newItemRow = copyRow(oldItemRow);
                newItemRow.hKey(hKey(newItemRow, newOrderRow, originalCustomer));
                testStore.deleteTestRow(oldItemRow);
                testStore.writeTestRow(newItemRow);
            }
        }
        startMonitoringHKeyPropagation();
        dbUpdate(updatedCustomer, originalCustomer);
        checkHKeyPropagation(2, 12);
        checkDB();
        checkInitialState();
    }

    @Test
    public void testCustomerPKUpdateCreatingDuplicate() throws Exception
    {
        // Set customer.cid = 11 for customer 23
        KeyUpdateRow originalCustomer = testStore.find(new HKey(vendorRT, 2L, 2L,
                                                           customerRT, 23L, 23L));
        KeyUpdateRow updatedCustomer = updateRow(originalCustomer, c_cid1, 11L, c_cid2, 11L, null);
        try {
            dbUpdate(originalCustomer, updatedCustomer);
            fail();
        } catch (InvalidOperationException e) {
            assertEquals(ErrorCode.DUPLICATE_KEY, e.getCode());
        }
        checkDB();
    }

    @Test
    public void testVendorPKUpdate() throws Exception
    {
        // Set vendor.vid = 0 for vendor 1
        KeyUpdateRow originalVendor = testStore.find(new HKey(vendorRT, 1L, 1L));
        KeyUpdateRow updatedVendor = updateRow(originalVendor, v_vid1, 0L, null);
        startMonitoringHKeyPropagation();
        dbUpdate(originalVendor, updatedVendor);
        checkHKeyPropagation(2, 0); // customer has vid, isn't affected by vendor update
        checkDB();
        // Revert change
        startMonitoringHKeyPropagation();
        dbUpdate(updatedVendor, originalVendor);
        checkHKeyPropagation(2, 0);
        checkDB();
        checkInitialState();
    }
    
    @Test
    public void testVendorPKUpdateCreatingDuplicate() throws Exception
    {
        // Set vendor.vid = 2 for vendor 1
        KeyUpdateRow originalVendorRow = testStore.find(new HKey(vendorRT, 1L, 1L));
        KeyUpdateRow updatedVendorRow = updateRow(originalVendorRow, v_vid1, 2L, v_vid2, 2L, null);
        try {
            dbUpdate(originalVendorRow, updatedVendorRow);
            fail();
        } catch (InvalidOperationException e) {
            assertEquals(ErrorCode.DUPLICATE_KEY,  e.getCode());
        }
        checkDB();
    }
    
    @Test
    public void testItemDelete() throws Exception
    {
        KeyUpdateRow itemRow = testStore.find(new HKey(vendorRT, 2L, 2L,
                                                  customerRT, 22L, 22L,
                                                  orderRT, 222L, 222L,
                                                  itemRT, 2222L, 2222L));
        startMonitoringHKeyPropagation();
        dbDelete(itemRow);
        checkHKeyPropagation(0, 0);
        checkDB();
        // Revert change
        startMonitoringHKeyPropagation();
        dbInsert(itemRow);
        checkHKeyPropagation(0, 0);
        checkDB();
        checkInitialState();
    }

    @Test
    public void testOrderDelete() throws Exception
    {
        KeyUpdateRow customerRow = testStore.find(new HKey(vendorRT, 2L, 2L,
                                                      customerRT, 22L, 22L));
        KeyUpdateRow orderRow = testStore.find(new HKey(vendorRT, 2L, 2L,
                                                   customerRT, 22L, 22L,
                                                   orderRT, 222L, 222L));
        // Propagate change to order 222's items to reflect db state
        for (long iid = 2221; iid <= 2223; iid++) {
            KeyUpdateRow oldItemRow = testStore.find(new HKey(vendorRT, 2L, 2L,
                                                         customerRT, 22L, 22L,
                                                         orderRT, 222L, 222L,
                                                         itemRT, iid, iid));
            KeyUpdateRow newItemRow = copyRow(oldItemRow);
            newItemRow.hKey(hKey(newItemRow, null));
            testStore.deleteTestRow(oldItemRow);
            testStore.writeTestRow(newItemRow);
        }
        startMonitoringHKeyPropagation();
        dbDelete(orderRow);
        checkHKeyPropagation(1, 3);
        checkDB();
        // Revert change
        for (long iid = 2221; iid <= 2223; iid++) {
            KeyUpdateRow oldItemRow = testStore.find(new HKey(vendorRT, null, null,
                                                         customerRT, null, null,
                                                         orderRT, 222L, 222L,
                                                         itemRT, iid, iid));
            KeyUpdateRow newItemRow = copyRow(oldItemRow);
            newItemRow.hKey(hKey(newItemRow, orderRow, customerRow));
            testStore.deleteTestRow(oldItemRow);
            testStore.writeTestRow(newItemRow);
        }
        startMonitoringHKeyPropagation();
        dbInsert(orderRow);
        checkHKeyPropagation(1, 3);
        checkDB();
        checkInitialState();
    }

    @Test
    public void testCustomerDelete() throws Exception
    {
        KeyUpdateRow customerRow = testStore.find(new HKey(vendorRT, 2L, 2L,
                                                      customerRT, 22L, 22L));
        // Propagate change to customer's descendents to reflect db state
        for (long oid = 221; oid <= 223; oid++) {
            KeyUpdateRow oldOrderRow = testStore.find(new HKey(vendorRT, 2L, 2L,
                                                          customerRT, 22L, 22L,
                                                          orderRT, oid, oid));
            KeyUpdateRow newOrderRow = copyRow(oldOrderRow);
            newOrderRow.hKey(hKey(newOrderRow, null));
            testStore.deleteTestRow(oldOrderRow);
            testStore.writeTestRow(newOrderRow);
            for (long iid = oid * 10 + 1; iid <= oid * 10 + 3; iid++) {
                KeyUpdateRow oldItemRow = testStore.find(new HKey(vendorRT, 2L, 2L,
                                                             customerRT, 22L, 22L,
                                                             orderRT, oid, oid,
                                                             itemRT, iid, iid));
                KeyUpdateRow newItemRow = copyRow(oldItemRow);
                newItemRow.hKey(hKey(newItemRow, newOrderRow));
                testStore.deleteTestRow(oldItemRow);
                testStore.writeTestRow(newItemRow);
            }
        }
        startMonitoringHKeyPropagation();
        dbDelete(customerRow);
        // 12; 3 orders, with 3 items each
        checkHKeyPropagation(1, 12);
        checkDB();
        // Revert change
        for (long oid = 221; oid <= 223; oid++) {
            KeyUpdateRow oldOrderRow = testStore.find(new HKey(vendorRT, null, null,
                                                          customerRT, 22L, 22L,
                                                          orderRT, oid, oid));
            KeyUpdateRow newOrderRow = copyRow(oldOrderRow);
            newOrderRow.hKey(hKey(newOrderRow, customerRow));
            testStore.deleteTestRow(oldOrderRow);
            testStore.writeTestRow(newOrderRow);
            for (long iid = oid * 10 + 1; iid <= oid * 10 + 3; iid++) {
                KeyUpdateRow oldItemRow = testStore.find(new HKey(vendorRT, null, null,
                                                             customerRT, 22L, 22L,
                                                             orderRT, oid, oid,
                                                             itemRT, iid, iid));
                KeyUpdateRow newItemRow = copyRow(oldItemRow);
                newItemRow.hKey(hKey(newItemRow, newOrderRow, customerRow));
                testStore.deleteTestRow(oldItemRow);
                testStore.writeTestRow(newItemRow);
            }
        }
        startMonitoringHKeyPropagation();
        dbInsert(customerRow);
        checkHKeyPropagation(1, 12);
        checkDB();
        checkInitialState();
    }
    
    @Test
    public void testVendorDelete() throws Exception
    {
        KeyUpdateRow vendorRow = testStore.find(new HKey(vendorRT, 1L, 1L));
        startMonitoringHKeyPropagation();
        dbDelete(vendorRow);
        // TODO: Why not apply the PDG optimization on inserts and deletes?
        checkHKeyPropagation(1, 39);
        checkDB();
        // Revert change
        startMonitoringHKeyPropagation();
        dbInsert(vendorRow);
        checkHKeyPropagation(1, 39);
        checkDB();
        checkInitialState();
    }

    @Override
    protected void createSchema() throws InvalidOperationException
    {
        // vendor
        vendorId = createTable("coi", "vendor",
                               "vid1 bigint not null",
                               "vx bigint",
                               "vid2 bigint not null",
                               "primary key(vid1, vid2)");
        v_vid1 = 0;
        v_vid2 = 2;
        v_vx = 1;
        // customer
        customerId = createTable("coi", "customer",
                                 "vid1 bigint",
                                 "cid2 bigint not null",
                                 "vid2 bigint",
                                 "cx bigint",
                                 "cid1 bigint not null",
                                 "primary key(cid1, cid2)",
                                 "grouping foreign key (vid1, vid2) references vendor(vid1, vid2)");
        c_cid1 = 4;
        c_cid2 = 1;
        c_vid1 = 0;
        c_vid2 = 2;
        c_cx = 3;
        // order
        orderId = createTable("coi", "order",
                              "cid1 bigint",
                              "cid2 bigint",
                              "oid2 bigint not null",
                              "oid1 bigint not null",
                              "ox bigint",
                              "priority bigint",
                              "when bigint",
                              "primary key(oid1, oid2)",
                              "unique(when)",
                              "grouping foreign key (cid1, cid2) references customer(cid1, cid2)");
        createIndex("coi", "order", "priority", "priority");
        o_oid1 = 3;
        o_oid2 = 2;
        o_cid1 = 0;
        o_cid2 = 1;
        o_ox = 4;
        o_priority = 5;
        o_when = 6;
        // item
        itemId = createTable("coi", "item",
                             "ix bigint",
                             "oid1 bigint",
                             "oid2 bigint",
                             "iid1 bigint not null",
                             "iid2 bigint not null",
                             "primary key(iid1, iid2)",
                             "grouping foreign key (oid1, oid2) references \"order\"(oid1, oid2)");
        i_iid1 = 3;
        i_iid2 = 4;
        i_oid1 = 1;
        i_oid2 = 2;
        i_ix = 0;
        // group
        vendorRT = getRowType(vendorId);
        customerRT = getRowType(customerId);
        orderRT = getRowType(orderId);
        itemRT = getRowType(itemId);
        group = customerRT.table().getGroup();
    }

    @Override
    protected List<List<Object>> vendorPKIndex(List<TreeRecord> records)
    {
        return indexFromRecords(records, vendorRT, 
                                v_vid1, v_vid2);
    }

    @Override
    protected List<List<Object>> customerPKIndex(List<TreeRecord> records)
    {
        return indexFromRecords(records, customerRT, 
                                c_cid1, c_cid2, 
                                c_vid1, c_vid2);
    }

    @Override
    protected List<List<Object>> orderPKIndex(List<TreeRecord> records)
    {
        return indexFromRecords(records, orderRT, 
                                o_oid1, o_oid2, 
                                HKeyElement.from(1), HKeyElement.from(2), 
                                o_cid1, o_cid2);
    }

    @Override
    protected List<List<Object>> itemPKIndex(List<TreeRecord> records)
    {
        return indexFromRecords(records, itemRT, 
                                i_iid1, i_iid2, 
                                HKeyElement.from(1), HKeyElement.from(2),
                                HKeyElement.from(4), HKeyElement.from(5),
                                i_oid1, i_oid2);
    }

    @Override
    protected List<List<Object>> orderPriorityIndex(List<TreeRecord> records)
    {
        return indexFromRecords(records, orderRT, 
                                o_priority, 
                                HKeyElement.from(1), HKeyElement.from(2),
                                o_cid1, o_cid2,
                                o_oid1, o_oid2);
    }

    @Override
    protected List<List<Object>> orderWhenIndex(List<TreeRecord> records)
    {
        return indexFromRecords(records, orderRT, 
                                o_when,
                                HKeyElement.from(1), HKeyElement.from(2),
                                o_cid1, o_cid2,
                                o_oid1, o_oid2);
    }

    // vendorRow, customerRow, orderRow and itemRow: The formatting of populateTables above is copied from 
    // KeyUpdateIT. Because of the duplicated column values and weird column orders, that formatting doesn't 
    // really work here. The functions below translate the rows, formatted as for the KeyUpdateIT schema, 
    // and generates the correct rows for this test.

    @Override
    protected void populateTables() throws Exception
    {
        // non-key vendor fields
        //     vx = vid * 100
        // non-key customer fields
        //     cx = cid * 100
        // non-key order fields
        //     ox = oid * 100
        //     priority = 8[1-3]
        //     when = unique, start counting at 9001
        // non-key item fields
        //     ix = iid * 100
        KeyUpdateRow customer;
        KeyUpdateRow order;
        //                               HKey reversed, value
        // Vendor 1
        dbInsert(                                           vendorRow(1,     100));
        //
        dbInsert(customer =                           customerRow(11, 1,     1100));
        dbInsert(order =                  orderRow(customer, 111, 11,        11100, 81, 9001));
        dbInsert(             itemRow(customer, order, 1111, 111,            111100));
        dbInsert(             itemRow(customer, order, 1112, 111,            111200));
        dbInsert(             itemRow(customer, order, 1113, 111,            111300));
        dbInsert(order =                  orderRow(customer, 112, 11,        11200, 83, 9002));
        dbInsert(             itemRow(customer, order, 1121, 112,            112100));
        dbInsert(             itemRow(customer, order, 1122, 112,            112200));
        dbInsert(             itemRow(customer, order, 1123, 112,            112300));
        dbInsert(order =                  orderRow(customer, 113, 11,        11300, 81, 9003));
        dbInsert(             itemRow(customer, order, 1131, 113,            113100));
        dbInsert(             itemRow(customer, order, 1132, 113,            113200));
        dbInsert(             itemRow(customer, order, 1133, 113,            113300));

        dbInsert(customer =                           customerRow(12, 1,     1200));
        dbInsert(order =                  orderRow(customer, 121, 12,        12100, 83, 9004));
        dbInsert(             itemRow(customer, order, 1211, 121,            121100));
        dbInsert(             itemRow(customer, order, 1212, 121,            121200));
        dbInsert(             itemRow(customer, order, 1213, 121,            121300));
        dbInsert(order =                  orderRow(customer, 122, 12,        12200, 81, 9005));
        dbInsert(             itemRow(customer, order, 1221, 122,            122100));
        dbInsert(             itemRow(customer, order, 1222, 122,            122200));
        dbInsert(             itemRow(customer, order, 1223, 122,            122300));
        dbInsert(order =                  orderRow(customer, 123, 12,        12300, 82, 9006));
        dbInsert(             itemRow(customer, order, 1231, 123,            123100));
        dbInsert(             itemRow(customer, order, 1232, 123,            123200));
        dbInsert(             itemRow(customer, order, 1233, 123,            123300));

        dbInsert(customer =                           customerRow(13, 1,     1300));
        dbInsert(order =                  orderRow(customer, 131, 13,        13100, 82, 9007));
        dbInsert(             itemRow(customer, order, 1311, 131,            131100));
        dbInsert(             itemRow(customer, order, 1312, 131,            131200));
        dbInsert(             itemRow(customer, order, 1313, 131,            131300));
        dbInsert(order =                  orderRow(customer, 132, 13,        13200, 83, 9008));
        dbInsert(             itemRow(customer, order, 1321, 132,            132100));
        dbInsert(             itemRow(customer, order, 1322, 132,            132200));
        dbInsert(             itemRow(customer, order, 1323, 132,            132300));
        dbInsert(order =                  orderRow(customer, 133, 13,        13300, 81, 9009));
        dbInsert(             itemRow(customer, order, 1331, 133,            133100));
        dbInsert(             itemRow(customer, order, 1332, 133,            133200));
        dbInsert(             itemRow(customer, order, 1333, 133,            133300));
        //
        // Vendor 2
        dbInsert(                                           vendorRow(2,     200));
        //
        dbInsert(customer =                           customerRow(21, 2,     2100));
        dbInsert(order =                  orderRow(customer, 211, 21,        21100, 81, 9010));
        dbInsert(             itemRow(customer, order, 2111, 211,            211100));
        dbInsert(             itemRow(customer, order, 2112, 211,            211200));
        dbInsert(             itemRow(customer, order, 2113, 211,            211300));
        dbInsert(order =                  orderRow(customer, 212, 21,        21200, 83, 9011));
        dbInsert(             itemRow(customer, order, 2121, 212,            212100));
        dbInsert(             itemRow(customer, order, 2122, 212,            212200));
        dbInsert(             itemRow(customer, order, 2123, 212,            212300));
        dbInsert(order =                  orderRow(customer, 213, 21,        21300, 82, 9012));
        dbInsert(             itemRow(customer, order, 2131, 213,            213100));
        dbInsert(             itemRow(customer, order, 2132, 213,            213200));
        dbInsert(             itemRow(customer, order, 2133, 213,            213300));

        dbInsert(customer =                           customerRow(22, 2,     2200));
        dbInsert(order =                  orderRow(customer, 221, 22,        22100, 82, 9013));
        dbInsert(             itemRow(customer, order, 2211, 221,            221100));
        dbInsert(             itemRow(customer, order, 2212, 221,            221200));
        dbInsert(             itemRow(customer, order, 2213, 221,            221300));
        dbInsert(order =                  orderRow(customer, 222, 22,        22200, 82, 9014));
        dbInsert(             itemRow(customer, order, 2221, 222,            222100));
        dbInsert(             itemRow(customer, order, 2222, 222,            222200));
        dbInsert(             itemRow(customer, order, 2223, 222,            222300));
        dbInsert(order =                  orderRow(customer, 223, 22,        22300, 81, 9015));
        dbInsert(             itemRow(customer, order, 2231, 223,            223100));
        dbInsert(             itemRow(customer, order, 2232, 223,            223200));
        dbInsert(             itemRow(customer, order, 2233, 223,            223300));

        dbInsert(customer =                           customerRow(23, 2,     2300));
        dbInsert(order =                  orderRow(customer, 231, 23,        23100, 82, 9016));
        dbInsert(             itemRow(customer, order, 2311, 231,            231100));
        dbInsert(             itemRow(customer, order, 2312, 231,            231200));
        dbInsert(             itemRow(customer, order, 2313, 231,            231300));
        dbInsert(order =                  orderRow(customer, 232, 23,        23200, 83, 9017));
        dbInsert(             itemRow(customer, order, 2321, 232,            232100));
        dbInsert(             itemRow(customer, order, 2322, 232,            232200));
        dbInsert(             itemRow(customer, order, 2323, 232,            232300));
        dbInsert(order =                  orderRow(customer, 233, 23,        23300, 81, 9018));
        dbInsert(             itemRow(customer, order, 2331, 233,            233100));
        dbInsert(             itemRow(customer, order, 2332, 233,            233200));
        dbInsert(             itemRow(customer, order, 2333, 233,            233300));
    }

    private KeyUpdateRow vendorRow(long vid, long vx)
    {
        KeyUpdateRow vendor = createTestRow(vendorRT, vid, vx, vid);
        vendor.hKey(new HKey(vendorRT, vid, vid));
        return vendor;
    }

    private KeyUpdateRow customerRow(long cid, long vid, long cx)
    {
        KeyUpdateRow customer = createTestRow(customerRT, vid, cid, vid, cx, cid);
        customer.hKey(new HKey(vendorRT, vid, vid, customerRT, cid, cid));
        return customer;
    }
    
    private KeyUpdateRow orderRow(KeyUpdateRow customer, long oid, long cid, long ox, long priority, long when)
    {
        KeyUpdateRow order = createTestRow(orderRT, cid, cid, oid, oid, ox, priority, when);
        order.hKey(new HKey(vendorRT, customer.value(c_vid1).getInt64(), customer.value(c_vid2).getInt64(),
                customerRT, cid, cid,
                orderRT, oid, oid));
        order.parent(customer);
        return order;
    }
    
    private KeyUpdateRow itemRow(KeyUpdateRow customer, KeyUpdateRow order, long iid, long oid, long ix)
    {
        KeyUpdateRow item = createTestRow(itemRT, ix, oid, oid, iid, iid);
        item.hKey(new HKey(vendorRT, customer.value(c_vid1).getInt64(), customer.value(c_vid2).getInt64(),
                customerRT, order.value(o_cid1).getInt64(), order.value(o_cid2).getInt64(),
                orderRT, oid, oid,
                itemRT, iid, iid));
        item.parent(order);
        return item;
    }

    @Override
    protected HKey hKey(KeyUpdateRow row)
    {
        HKey hKey = null;
        RowType rowType = row.rowType();
        if (rowType == vendorRT) {
            hKey = new HKey(vendorRT, row.value(v_vid1).getInt64(), row.value(v_vid2).getInt64());
        } else if (rowType == customerRT) {
            hKey = new HKey(vendorRT, row.value(c_vid1).getInt64(), row.value(c_vid2).getInt64(), 
                            customerRT, row.value(c_cid1).getInt64(), row.value(c_cid2).getInt64());
        } else if (rowType == orderRT) {
            assertNotNull(row.parent());
            hKey = new HKey(vendorRT, row.parent().value(c_vid1).getInt64(), row.parent().value(c_vid2).getInt64(),
                            customerRT, row.value(o_cid1).getInt64(), row.value(o_cid2).getInt64(),
                            orderRT, row.value(o_oid1).getInt64(), row.value(o_oid2).getInt64());
        } else if (rowType == itemRT) {
            assertNotNull(row.parent());
            assertNotNull(row.parent().parent());
            hKey = new HKey(vendorRT, row.parent().parent().value(c_vid1).getInt64(), row.parent().parent().value(c_vid2).getInt64(),
                            customerRT, row.parent().value(o_cid1).getInt64(), row.parent().value(o_cid2).getInt64(),
                            orderRT, row.value(i_oid1).getInt64(), row.value(i_oid2).getInt64(),
                            itemRT, row.value(i_iid1).getInt64(), row.value(i_iid2).getInt64());
        } else {
            fail();
        }
        return hKey;
    }
    
    @Override
    protected boolean checkChildPKs() {
        return true;
    }

    @Override
    protected HKey hKey(KeyUpdateRow row, KeyUpdateRow parent, KeyUpdateRow grandparent)
    {
        HKey hKey = null;
        RowType rowType = row.rowType();
        if (rowType == vendorRT) {
            hKey = new HKey(vendorRT, row.value(v_vid1).getInt64(), row.value(v_vid2).getInt64());
        } else if (rowType == customerRT) {
            hKey = new HKey(vendorRT, row.value(c_vid1).getInt64(), row.value(c_vid2).getInt64(),
                            customerRT, row.value(c_cid1).getInt64(), row.value(c_cid2).getInt64());
        } else if (rowType == orderRT) {
            hKey = new HKey(vendorRT, parent == null ? null : parent.value(c_vid1).getInt64(), parent == null ? null : parent.value(c_vid2).getInt64(),
                            customerRT, row.value(o_cid1).getInt64(), row.value(o_cid2).getInt64(),
                            orderRT, row.value(o_oid1).getInt64(), row.value(o_oid2).getInt64());
        } else if (rowType == itemRT) {
            hKey = new HKey(vendorRT, grandparent == null ? null : grandparent.value(c_vid1).getInt64(), grandparent == null ? null : grandparent.value(c_vid2).getInt64(),
                            customerRT, parent == null ? null : parent.value(o_cid1).getInt64(), parent == null ? null : parent.value(o_cid2).getInt64(),
                            orderRT, row.value(i_oid1).getInt64(), row.value(i_oid2).getInt64(),
                            itemRT, row.value(i_iid1).getInt64(), row.value(i_iid2).getInt64());
        } else {
            fail();
        }
        row.parent(parent);
        return hKey;
    }

    @Override
    protected void checkInitialState(Row row)
    {
        RowType rowType = row.rowType();
        if (rowType == vendorRT) {
            assertEquals(row.value(v_vx).getInt64(), row.value(v_vid1).getInt64() * 100);
            assertEquals(row.value(v_vx).getInt64(), row.value(v_vid2).getInt64() * 100);
        } else if (rowType == customerRT) {
            assertEquals(row.value(c_cx).getInt64(), row.value(c_cid1).getInt64() * 100);
            assertEquals(row.value(c_cx).getInt64(), row.value(c_cid2).getInt64() * 100);
        } else if (rowType == orderRT) {
            assertEquals(row.value(o_cid1).getInt64(), row.value(o_oid1).getInt64()/ 10);
            assertEquals(row.value(o_cid2).getInt64(), row.value(o_oid2).getInt64() / 10);
            assertEquals(row.value(o_ox).getInt64(), row.value(o_oid1).getInt64() * 100);
            assertEquals(row.value(o_ox).getInt64(), row.value(o_oid2).getInt64() * 100);
        } else if (rowType == itemRT) {
            assertEquals(row.value(i_oid1).getInt64(), row.value(i_iid1).getInt64() / 10);
            assertEquals(row.value(i_oid2).getInt64(), row.value(i_iid2).getInt64() / 10);
            assertEquals(row.value(i_ix).getInt64(), row.value(i_iid1).getInt64() * 100);
            assertEquals(row.value(i_ix).getInt64(), row.value(i_iid2).getInt64() * 100);
        } else {
            fail();
        }
    }
    
    @Override
    protected void confirmColumns()
    {
        confirmColumn(vendorRT, v_vid1, "vid1");
        confirmColumn(vendorRT, v_vid2, "vid2");
        confirmColumn(vendorRT, v_vx, "vx");

        confirmColumn(customerRT, c_cid1, "cid1");
        confirmColumn(customerRT, c_cid2, "cid2");
        confirmColumn(customerRT, c_vid1, "vid1");
        confirmColumn(customerRT, c_vid2, "vid2");
        confirmColumn(customerRT, c_cx, "cx");

        confirmColumn(orderRT, o_oid1, "oid1");
        confirmColumn(orderRT, o_oid2, "oid2");
        confirmColumn(orderRT, o_cid1, "cid1");
        confirmColumn(orderRT, o_cid2, "cid2");
        confirmColumn(orderRT, o_ox, "ox");
        confirmColumn(orderRT, o_priority, "priority");
        confirmColumn(orderRT, o_when, "when");

        confirmColumn(itemRT, i_iid1, "iid1");
        confirmColumn(itemRT, i_iid2, "iid2");
        confirmColumn(itemRT, i_oid1, "oid1");
        confirmColumn(itemRT, i_oid2, "oid2");
        confirmColumn(itemRT, i_ix, "ix");
    }
}
