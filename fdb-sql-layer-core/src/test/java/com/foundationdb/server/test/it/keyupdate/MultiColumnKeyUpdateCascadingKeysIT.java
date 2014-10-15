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

import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.rowdata.RowDef;
import org.junit.Test;

import java.util.List;

import static com.foundationdb.server.test.it.keyupdate.Schema.*;
import static org.junit.Assert.*;

// Like KeyUpdateCascadingIT, but with multi-column keys

public class MultiColumnKeyUpdateCascadingKeysIT extends KeyUpdateBase
{
    @Test
    public void testItemFKUpdate() throws Exception
    {
        // Set item.oid1 = 0 for item 1222
        TestRow originalItem = testStore.find(new HKey(vendorRD, 1L, 1L,
                                                       customerRD, 12L, 12L,
                                                       orderRD, 122L, 122L,
                                                       itemRD, 1222L, 1222L));
        TestRow updatedItem = copyRow(originalItem);
        updateRow(updatedItem, i_oid1, 0L);
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
        TestRow originalItem = testStore.find(new HKey(vendorRD, 1L, 1L,
                                                       customerRD, 12L, 12L,
                                                       orderRD, 122L, 122L,
                                                       itemRD, 1222L, 1222L));
        TestRow updatedItem = copyRow(originalItem);
        updateRow(updatedItem, i_iid2, 1221L);
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
        TestRow originalItem = testStore.find(new HKey(vendorRD, 1L, 1L,
                                                       customerRD, 12L, 12L,
                                                       orderRD, 122L, 122L,
                                                       itemRD, 1222L, 1222L));
        TestRow updatedItem = copyRow(originalItem);
        updateRow(updatedItem, i_iid1, 1223L);
        updateRow(updatedItem, i_iid2, 1223L);
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
        TestRow originalOrder = testStore.find(new HKey(vendorRD, 2L, 2L,
                                                        customerRD, 22L, 22L,
                                                        orderRD, 222L, 222L));
        TestRow updatedOrder = copyRow(originalOrder);
        updateRow(updatedOrder, o_cid1, 24L, null);
        // Propagate change to order 222's items to reflect db state
        for (long iid = 2221; iid <= 2223; iid++) {
            TestRow oldItemRow = testStore.find(new HKey(vendorRD, 2L, 2L,
                                                         customerRD, 22L, 22L,
                                                         orderRD, 222L, 222L,
                                                         itemRD, iid, iid));
            TestRow newItemRow = copyRow(oldItemRow);
            newItemRow.hKey(hKey(newItemRow, updatedOrder));
            testStore.deleteTestRow(oldItemRow);
            testStore.writeTestRow(newItemRow);
        }
        startMonitoringHKeyPropagation();
        dbUpdate(originalOrder, updatedOrder);
        checkHKeyPropagation(2, 0);
        checkDB();
        // Revert change
        for (long iid = 2221; iid <= 2223; iid++) {
            TestRow oldItemRow = testStore.find(new HKey(vendorRD, 2L, 2L,
                                                         customerRD, 22L, 22L,
                                                         orderRD, 222L, 222L,
                                                         itemRD, iid, iid));
            TestRow newItemRow = copyRow(oldItemRow);
            newItemRow.hKey(hKey(newItemRow, originalOrder));
            testStore.deleteTestRow(oldItemRow);
            testStore.writeTestRow(newItemRow);
        }
        startMonitoringHKeyPropagation();
        dbUpdate(updatedOrder, originalOrder);
        checkHKeyPropagation(2, 0);
        checkDB();
        checkInitialState();
    }

    @Test
    public void testOrderPKUpdate() throws Exception
    {
        // Set order.oid = 0 for order 222
        TestRow originalOrder = testStore.find(new HKey(vendorRD, 2L, 2L,
                                                        customerRD, 22L, 22L,
                                                        orderRD, 222L, 222L));
        TestRow udpatedOrder = copyRow(originalOrder);
        updateRow(udpatedOrder, o_oid1, 0L);
        updateRow(udpatedOrder, o_oid2, 0L);
        // Propagate change to order 222's items to reflect db state
        for (long iid = 2221; iid <= 2223; iid++) {
            TestRow oldItemRow = testStore.find(new HKey(vendorRD, 2L, 2L,
                                                         customerRD, 22L, 22L,
                                                         orderRD, 222L, 222L,
                                                         itemRD, iid, iid));
            TestRow newItemRow = copyRow(oldItemRow);
            newItemRow.hKey(hKey(newItemRow, null));
            testStore.deleteTestRow(oldItemRow);
            testStore.writeTestRow(newItemRow);
        }
        startMonitoringHKeyPropagation();
        dbUpdate(originalOrder, udpatedOrder);
        checkHKeyPropagation(2, 0);
        checkDB();
        // Revert change
        for (long iid = 2221; iid <= 2223; iid++) {
            TestRow oldItemRow = testStore.find(new HKey(vendorRD, 2L, 2L,
                                                         customerRD, 22L, 22L,
                                                         orderRD, 222L, 222L,
                                                         itemRD, iid, iid));
            TestRow newItemRow = copyRow(oldItemRow);
            newItemRow.hKey(hKey(newItemRow, originalOrder));
            testStore.deleteTestRow(oldItemRow);
            testStore.writeTestRow(newItemRow);
        }
        startMonitoringHKeyPropagation();
        dbUpdate(udpatedOrder, originalOrder);
        checkHKeyPropagation(2, 0);
        checkDB();
        checkInitialState();
    }

    @Test
    public void testOrderPKUpdateCreatingDuplicate() throws Exception
    {
        // Set order.oid = 221 for order 222
        TestRow originalOrder = testStore.find(new HKey(vendorRD, 2L, 2L,
                                                        customerRD, 22L, 22L,
                                                        orderRD, 222L, 222L));
        TestRow updatedOrder = copyRow(originalOrder);
        updateRow(updatedOrder, o_oid1, 221L);
        updateRow(updatedOrder, o_oid2, 221L);
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
        TestRow originalCustomer = testStore.find(new HKey(vendorRD, 1L, 1L,
                                                           customerRD, 13L, 13L));
        TestRow udpatedCustomer = copyRow(originalCustomer);
        updateRow(udpatedCustomer, c_vid1, 0L, null);
        updateRow(udpatedCustomer, c_vid2, 0L, null);
        // Propagate change to customer 13s descendents to reflect db state
        for (long oid = 131; oid <= 133; oid++) {
            TestRow oldOrderRow = testStore.find(new HKey(vendorRD, 1L, 1L,
                                                          customerRD, 13L, 13L,
                                                          orderRD, oid, oid));
            TestRow newOrderRow = copyRow(oldOrderRow);
            newOrderRow.hKey(hKey(newOrderRow, udpatedCustomer));
            testStore.deleteTestRow(oldOrderRow);
            testStore.writeTestRow(newOrderRow);
            for (long iid = oid * 10 + 1; iid <= oid * 10 + 3; iid++) {
                TestRow oldItemRow = testStore.find(new HKey(vendorRD, 1L, 1L,
                                                             customerRD, 13L, 13L,
                                                             orderRD, oid, oid,
                                                             itemRD, iid, iid));
                TestRow newItemRow = copyRow(oldItemRow);
                newItemRow.hKey(hKey(newItemRow, newOrderRow, udpatedCustomer));
                testStore.deleteTestRow(oldItemRow);
                testStore.writeTestRow(newItemRow);
            }
        }
        startMonitoringHKeyPropagation();
        dbUpdate(originalCustomer, udpatedCustomer);
        checkHKeyPropagation(2, 0);
        checkDB();
        // Revert change
        for (long oid = 131; oid <= 133; oid++) {
            TestRow oldOrderRow = testStore.find(new HKey(vendorRD, 1L, 1L,
                                                          customerRD, 13L, 13L,
                                                          orderRD, oid, oid));
            TestRow newOrderRow = copyRow(oldOrderRow);
            newOrderRow.hKey(hKey(newOrderRow, originalCustomer));
            testStore.deleteTestRow(oldOrderRow);
            testStore.writeTestRow(newOrderRow);
            for (long iid = oid * 10 + 1; iid <= oid * 10 + 3; iid++) {
                TestRow oldItemRow = testStore.find(new HKey(vendorRD, 1L, 1L,
                                                             customerRD, 13L, 13L,
                                                             orderRD, oid, oid,
                                                             itemRD, iid, iid));
                TestRow newItemRow = copyRow(oldItemRow);
                newItemRow.hKey(hKey(newItemRow, newOrderRow, originalCustomer));
                testStore.deleteTestRow(oldItemRow);
                testStore.writeTestRow(newItemRow);
            }
        }
        startMonitoringHKeyPropagation();
        dbUpdate(udpatedCustomer, originalCustomer);
        checkHKeyPropagation(2, 0);
        checkDB();
        checkInitialState();
    }

    @Test
    public void testCustomerPKUpdate() throws Exception
    {
        // Set customer.cid = 0 for customer 22
        TestRow originalCustomer = testStore.find(new HKey(vendorRD, 2L, 2L,
                                                           customerRD, 22L, 22L));
        TestRow updatedCustomer = copyRow(originalCustomer);
        updateRow(updatedCustomer, c_cid2, 23L, null);
        // Propagate change to customer 22's orders and items to reflect db state
        for (long oid = 221; oid <= 223; oid++) {
            TestRow oldOrderRow = testStore.find(new HKey(vendorRD, 2L, 2L,
                                                          customerRD, 22L, 22L,
                                                          orderRD, oid, oid));
            TestRow newOrderRow = copyRow(oldOrderRow);
            newOrderRow.hKey(hKey(newOrderRow, null));
            testStore.deleteTestRow(oldOrderRow);
            testStore.writeTestRow(newOrderRow);
            for (long iid = oid * 10 + 1; iid <= oid * 10 + 3; iid++) {
                TestRow oldItemRow = testStore.find(new HKey(vendorRD, 2L, 2L,
                                                             customerRD, 22L, 22L,
                                                             orderRD, oid, oid,
                                                             itemRD, iid, iid));
                TestRow newItemRow = copyRow(oldItemRow);
                newItemRow.hKey(hKey(newItemRow, newOrderRow));
                testStore.deleteTestRow(oldItemRow);
                testStore.writeTestRow(newItemRow);
            }
        }
        startMonitoringHKeyPropagation();
        dbUpdate(originalCustomer, updatedCustomer);
        checkHKeyPropagation(2, 0);
        checkDB();
        // Revert change
        for (long oid = 221; oid <= 223; oid++) {
            TestRow oldOrderRow = testStore.find(new HKey(vendorRD, 2L, 2L,
                                                          customerRD, 22L, 22L,
                                                          orderRD, oid, oid));
            TestRow newOrderRow = copyRow(oldOrderRow);
            newOrderRow.hKey(hKey(newOrderRow, originalCustomer));
            testStore.deleteTestRow(oldOrderRow);
            testStore.writeTestRow(newOrderRow);
            for (long iid = oid * 10 + 1; iid <= oid * 10 + 3; iid++) {
                TestRow oldItemRow = testStore.find(new HKey(vendorRD, 2L, 2L,
                                                             customerRD, 22L, 22L,
                                                             orderRD, oid, oid,
                                                             itemRD, iid, iid));
                TestRow newItemRow = copyRow(oldItemRow);
                newItemRow.hKey(hKey(newItemRow, newOrderRow, originalCustomer));
                testStore.deleteTestRow(oldItemRow);
                testStore.writeTestRow(newItemRow);
            }
        }
        startMonitoringHKeyPropagation();
        dbUpdate(updatedCustomer, originalCustomer);
        checkHKeyPropagation(2, 0);
        checkDB();
        checkInitialState();
    }

    @Test
    public void testCustomerPKUpdateCreatingDuplicate() throws Exception
    {
        // Set customer.cid = 11 for customer 23
        TestRow originalCustomer = testStore.find(new HKey(vendorRD, 2L, 2L,
                                                           customerRD, 23L, 23L));
        TestRow updatedCustomer = copyRow(originalCustomer);
        updateRow(updatedCustomer, c_cid1, 21L);
        updateRow(updatedCustomer, c_cid2, 21L);
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
        TestRow originalVendor = testStore.find(new HKey(vendorRD, 1L, 1L));
        TestRow updatedVendor = copyRow(originalVendor);
        updateRow(updatedVendor, v_vid1, 0L, null);
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
        TestRow originalVendorRow = testStore.find(new HKey(vendorRD, 1L, 1L));
        TestRow updatedVendorRow = copyRow(originalVendorRow);
        updateRow(updatedVendorRow, v_vid1, 2L, null);
        updateRow(updatedVendorRow, v_vid2, 2L, null);
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
        TestRow itemRow = testStore.find(new HKey(vendorRD, 2L, 2L,
                                                  customerRD, 22L, 22L,
                                                  orderRD, 222L, 222L,
                                                  itemRD, 2222L, 2222L));
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
        TestRow customerRow = testStore.find(new HKey(vendorRD, 2L, 2L,
                                                      customerRD, 22L, 22L));
        TestRow orderRow = testStore.find(new HKey(vendorRD, 2L, 2L,
                                                   customerRD, 22L, 22L,
                                                   orderRD, 222L, 222L));
        // Propagate change to order 222's items to reflect db state
        for (long iid = 2221; iid <= 2223; iid++) {
            TestRow oldItemRow = testStore.find(new HKey(vendorRD, 2L, 2L,
                                                         customerRD, 22L, 22L,
                                                         orderRD, 222L, 222L,
                                                         itemRD, iid, iid));
            TestRow newItemRow = copyRow(oldItemRow);
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
            TestRow oldItemRow = testStore.find(new HKey(vendorRD, 2L, 2L,
                                                         customerRD, 22L, 22L,
                                                         orderRD, 222L, 222L,
                                                         itemRD, iid, iid));
            TestRow newItemRow = copyRow(oldItemRow);
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
        TestRow customerRow = testStore.find(new HKey(vendorRD, 2L, 2L,
                                                      customerRD, 22L, 22L));
        // Propagate change to customer's descendents to reflect db state
        for (long oid = 221; oid <= 223; oid++) {
            TestRow oldOrderRow = testStore.find(new HKey(vendorRD, 2L, 2L,
                                                          customerRD, 22L, 22L,
                                                          orderRD, oid, oid));
            TestRow newOrderRow = copyRow(oldOrderRow);
            newOrderRow.hKey(hKey(newOrderRow));
            testStore.deleteTestRow(oldOrderRow);
            testStore.writeTestRow(newOrderRow);
            for (long iid = oid * 10 + 1; iid <= oid * 10 + 3; iid++) {
                TestRow oldItemRow = testStore.find(new HKey(vendorRD, 2L, 2L,
                                                             customerRD, 22L, 22L,
                                                             orderRD, oid, oid,
                                                             itemRD, iid, iid));
                TestRow newItemRow = copyRow(oldItemRow);
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
            TestRow oldOrderRow = testStore.find(new HKey(vendorRD, 2L, 2L,
                                                          customerRD, 22L, 22L,
                                                          orderRD, oid, oid));
            TestRow newOrderRow = copyRow(oldOrderRow);
            newOrderRow.hKey(hKey(newOrderRow, customerRow));
            testStore.deleteTestRow(oldOrderRow);
            testStore.writeTestRow(newOrderRow);
            for (long iid = oid * 10 + 1; iid <= oid * 10 + 3; iid++) {
                TestRow oldItemRow = testStore.find(new HKey(vendorRD, 2L, 2L,
                                                             customerRD, 22L, 22L,
                                                             orderRD, oid, oid,
                                                             itemRD, iid, iid));
                TestRow newItemRow = copyRow(oldItemRow);
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
        TestRow vendorRow = testStore.find(new HKey(vendorRD, 1L, 1L));
        startMonitoringHKeyPropagation();
        dbDelete(vendorRow);
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
                                 "vid1 bigint not null",
                                 "cid2 bigint not null",
                                 "vid2 bigint not null",
                                 "cx bigint",
                                 "cid1 bigint not null",
                                 "primary key(vid1, vid2, cid1, cid2)",
                                 "grouping foreign key (vid1, vid2) references vendor(vid1, vid2)");
        c_vid1 = 0;
        c_vid2 = 2;
        c_cid1 = 4;
        c_cid2 = 1;
        c_cx = 3;
        // order
        orderId = createTable("coi", "order",
                              "vid2 bigint not null",
                              "cid1 bigint not null",
                              "cid2 bigint not null",
                              "oid2 bigint not null",
                              "oid1 bigint not null",
                              "ox bigint",
                              "priority bigint",
                              "vid1 bigint not null",
                              "when bigint",
                              "primary key(vid1, vid2, cid1, cid2, oid1, oid2)",
                              "unique(when)",
                              "grouping foreign key (vid1, vid2, cid1, cid2) references customer(vid1, vid2, cid1, cid2)");
        createIndex("coi", "order", "priority", "priority");
        o_vid1 = 7;
        o_vid2 = 0;
        o_cid1 = 1;
        o_cid2 = 2;
        o_oid1 = 4;
        o_oid2 = 3;
        o_ox = 5;
        o_priority = 6;
        o_when = 8;
        // item
        itemId = createTable("coi", "item",
                             "cid1 bigint not null",
                             "ix bigint",
                             "oid1 bigint not null",
                             "vid2 bigint not null",
                             "oid2 bigint not null",
                             "vid1 bigint not null",
                             "cid2 bigint not null",
                             "iid1 bigint not null",
                             "iid2 bigint not null",
                             "primary key(vid1, vid2, cid1, cid2, oid1, oid2, iid1, iid2)",
                             "grouping foreign key (vid1, vid2, cid1, cid2, oid1, oid2) references \"order\"(vid1, vid2, cid1, cid2, oid1, oid2)");
        i_vid1 = 5;
        i_vid2 = 3;
        i_cid1 = 0;
        i_cid2 = 6;
        i_oid1 = 2;
        i_oid2 = 4;
        i_iid1 = 7;
        i_iid2 = 8;
        i_ix = 1;
        // group
        vendorRD = getRowDef(vendorId);
        customerRD = getRowDef(customerId);
        orderRD = getRowDef(orderId);
        itemRD = getRowDef(itemId);
        group = customerRD.getGroup();
    }

    @Override
    protected List<List<Object>> vendorPKIndex(List<TreeRecord> records)
    {
        return indexFromRecords(records, vendorRD,
                                v_vid1, v_vid2);
    }

    @Override
    protected List<List<Object>> customerPKIndex(List<TreeRecord> records)
    {
        return indexFromRecords(records, customerRD,
                                c_vid1, c_vid2,
                                c_cid1, c_cid2);
    }

    @Override
    protected List<List<Object>> orderPKIndex(List<TreeRecord> records)
    {
        return indexFromRecords(records, orderRD,
                                o_vid1, o_vid2,
                                o_cid1, o_cid2,
                                o_oid1, o_oid2);
    }

    @Override
    protected List<List<Object>> itemPKIndex(List<TreeRecord> records)
    {
        return indexFromRecords(records, itemRD,
                                i_vid1, i_vid2,
                                i_cid1, i_cid2,
                                i_oid1, i_oid2,
                                i_iid1, i_iid2);
    }

    @Override
    protected List<List<Object>> orderPriorityIndex(List<TreeRecord> records)
    {
        return indexFromRecords(records, orderRD,
                                o_priority,
                                o_vid1, o_vid2,
                                o_cid1, o_cid2,
                                o_oid1, o_oid2);
    }

    @Override
    protected List<List<Object>> orderWhenIndex(List<TreeRecord> records)
    {
        return indexFromRecords(records, orderRD,
                                o_when,
                                o_vid1, o_vid2,
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
        TestRow customer;
        TestRow order;
        // Vendor 1
        dbInsert(  vendorRow(1,                           100));
        //
        dbInsert(customerRow(1, 11,                       1100));
        dbInsert(   orderRow(1, 11, 111,                  11100, 81, 9001));
        dbInsert(    itemRow(1, 11, 111, 1111,            111100));
        dbInsert(    itemRow(1, 11, 111, 1112,            111200));
        dbInsert(    itemRow(1, 11, 111, 1113,            111300));
        dbInsert(   orderRow(1, 11, 112,                  11200, 83, 9002));
        dbInsert(    itemRow(1, 11, 112, 1121,            112100));
        dbInsert(    itemRow(1, 11, 112, 1122,            112200));
        dbInsert(    itemRow(1, 11, 112, 1123,            112300));
        dbInsert(   orderRow(1, 11, 113,                  11300, 81, 9003));
        dbInsert(    itemRow(1, 11, 113, 1131,            113100));
        dbInsert(    itemRow(1, 11, 113, 1132,            113200));
        dbInsert(    itemRow(1, 11, 113, 1133,            113300));

        dbInsert(customerRow(1, 12,                       1200));
        dbInsert(   orderRow(1, 12, 121,                  12100, 83, 9004));
        dbInsert(    itemRow(1, 12, 121, 1211,            121100));
        dbInsert(    itemRow(1, 12, 121, 1212,            121200));
        dbInsert(    itemRow(1, 12, 121, 1213,            121300));
        dbInsert(   orderRow(1, 12, 122,                  12200, 81, 9005));
        dbInsert(    itemRow(1, 12, 122, 1221,            122100));
        dbInsert(    itemRow(1, 12, 122, 1222,            122200));
        dbInsert(    itemRow(1, 12, 122, 1223,            122300));
        dbInsert(   orderRow(1, 12, 123,                  12300, 82, 9006));
        dbInsert(    itemRow(1, 12, 123, 1231,            123100));
        dbInsert(    itemRow(1, 12, 123, 1232,            123200));
        dbInsert(    itemRow(1, 12, 123, 1233,            123300));

        dbInsert(customerRow(1, 13,                       1300));
        dbInsert(   orderRow(1, 13, 131,                  13100, 82, 9007));
        dbInsert(    itemRow(1, 13, 131, 1311,            131100));
        dbInsert(    itemRow(1, 13, 131, 1312,            131200));
        dbInsert(    itemRow(1, 13, 131, 1313,            131300));
        dbInsert(   orderRow(1, 13, 132,                  13200, 83, 9008));
        dbInsert(    itemRow(1, 13, 132, 1321,            132100));
        dbInsert(    itemRow(1, 13, 132, 1322,            132200));
        dbInsert(    itemRow(1, 13, 132, 1323,            132300));
        dbInsert(   orderRow(1, 13, 133,                  13300, 81, 9009));
        dbInsert(    itemRow(1, 13, 133, 1331,            133100));
        dbInsert(    itemRow(1, 13, 133, 1332,            133200));
        dbInsert(    itemRow(1, 13, 133, 1333,            133300));
        //
        // Vendor 2
        dbInsert(  vendorRow(2,                           200));
        //
        dbInsert(customerRow(2, 21,                       2100));
        dbInsert(   orderRow(2, 21, 211,                  21100, 81, 9010));
        dbInsert(    itemRow(2, 21, 211, 2111,            211100));
        dbInsert(    itemRow(2, 21, 211, 2112,            211200));
        dbInsert(    itemRow(2, 21, 211, 2113,            211300));
        dbInsert(   orderRow(2, 21, 212,                  21200, 83, 9011));
        dbInsert(    itemRow(2, 21, 212, 2121,            212100));
        dbInsert(    itemRow(2, 21, 212, 2122,            212200));
        dbInsert(    itemRow(2, 21, 212, 2123,            212300));
        dbInsert(   orderRow(2, 21, 213,                  21300, 82, 9012));
        dbInsert(    itemRow(2, 21, 213, 2131,            213100));
        dbInsert(    itemRow(2, 21, 213, 2132,            213200));
        dbInsert(    itemRow(2, 21, 213, 2133,            213300));

        dbInsert(customerRow(2, 22,                       2200));
        dbInsert(   orderRow(2, 22, 221,                  22100, 82, 9013));
        dbInsert(    itemRow(2, 22, 221, 2211,            221100));
        dbInsert(    itemRow(2, 22, 221, 2212,            221200));
        dbInsert(    itemRow(2, 22, 221, 2213,            221300));
        dbInsert(   orderRow(2, 22, 222,                  22200, 82, 9014));
        dbInsert(    itemRow(2, 22, 222, 2221,            222100));
        dbInsert(    itemRow(2, 22, 222, 2222,            222200));
        dbInsert(    itemRow(2, 22, 222, 2223,            222300));
        dbInsert(   orderRow(2, 22, 223,                  22300, 81, 9015));
        dbInsert(    itemRow(2, 22, 223, 2231,            223100));
        dbInsert(    itemRow(2, 22, 223, 2232,            223200));
        dbInsert(    itemRow(2, 22, 223, 2233,            223300));

        dbInsert(customerRow(2, 23,                       2300));
        dbInsert(   orderRow(2, 23, 231,                  23100, 82, 9016));
        dbInsert(    itemRow(2, 23, 231, 2311,            231100));
        dbInsert(    itemRow(2, 23, 231, 2312,            231200));
        dbInsert(    itemRow(2, 23, 231, 2313,            231300));
        dbInsert(   orderRow(2, 23, 232,                  23200, 83, 9017));
        dbInsert(    itemRow(2, 23, 232, 2321,            232100));
        dbInsert(    itemRow(2, 23, 232, 2322,            232200));
        dbInsert(    itemRow(2, 23, 232, 2323,            232300));
        dbInsert(   orderRow(2, 23, 233,                  23300, 81, 9018));
        dbInsert(    itemRow(2, 23, 233, 2331,            233100));
        dbInsert(    itemRow(2, 23, 233, 2332,            233200));
        dbInsert(    itemRow(2, 23, 233, 2333,            233300));
    }

    private TestRow vendorRow(long vid, long vx)
    {
        TestRow vendor = createTestRow(vendorId);
        vendor.put(v_vid1, vid);
        vendor.put(v_vid2, vid);
        vendor.put(v_vx, vx);
        vendor.hKey(new HKey(vendorRD, vid, vid));
        return vendor;
    }

    private TestRow customerRow(long vid, long cid, long cx)
    {
        TestRow customer = createTestRow(customerId);
        customer.put(c_vid1, vid);
        customer.put(c_vid2, vid);
        customer.put(c_cid1, cid);
        customer.put(c_cid2, cid);
        customer.put(c_cx, cx);
        customer.hKey(new HKey(vendorRD, vid, vid, customerRD, cid, cid));
        return customer;
    }

    private TestRow orderRow(long vid, long cid, long oid, long ox, long priority, long when)
    {
        TestRow order = createTestRow(orderId);
        order.put(o_vid1, vid);
        order.put(o_vid2, vid);
        order.put(o_cid1, cid);
        order.put(o_cid2, cid);
        order.put(o_oid1, oid);
        order.put(o_oid2, oid);
        order.put(o_ox, ox);
        order.put(o_priority, priority);
        order.put(o_when, when);
        order.hKey(new HKey(vendorRD, vid, vid,
                            customerRD, cid, cid,
                            orderRD, oid, oid));
        return order;
    }

    private TestRow itemRow(long vid, long cid, long oid, long iid, long ix)
    {
        TestRow item = createTestRow(itemId);
        item.put(i_vid1, vid);
        item.put(i_vid2, vid);
        item.put(i_cid1, cid);
        item.put(i_cid2, cid);
        item.put(i_oid1, oid);
        item.put(i_oid2, oid);
        item.put(i_iid1, iid);
        item.put(i_iid2, iid);
        item.put(i_ix, ix);
        item.hKey(new HKey(vendorRD, vid, vid,
                           customerRD, cid, cid,
                           orderRD, oid, oid,
                           itemRD, iid, iid));
        return item;
    }

    @Override
    protected HKey hKey(TestRow row)
    {
        HKey hKey = null;
        RowDef rowDef = row.getRowDef();
        if (rowDef == vendorRD) {
            hKey = new HKey(vendorRD, row.get(v_vid1), row.get(v_vid2));
        } else if (rowDef == customerRD) {
            hKey = new HKey(vendorRD, row.get(c_vid1), row.get(c_vid2), 
                            customerRD, row.get(c_cid1), row.get(c_cid2));
        } else if (rowDef == orderRD) {
            hKey = new HKey(vendorRD, row.get(o_vid1), row.get(o_vid2),
                            customerRD, row.get(o_cid1), row.get(o_cid2),
                            orderRD, row.get(o_oid1), row.get(o_oid2));
        } else if (rowDef == itemRD) {
            hKey = new HKey(vendorRD, row.get(i_vid1), row.get(i_vid1),
                            customerRD, row.get(i_cid1), row.get(i_cid2),
                            orderRD, row.get(i_oid1), row.get(i_oid2),
                            itemRD, row.get(i_iid1), row.get(i_iid2));
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
    protected HKey hKey(TestRow row, TestRow parent, TestRow grandparent)
    {
        return hKey(row);
    }

    protected void checkInitialState(NewRow row)
    {
        RowDef rowDef = row.getRowDef();
        if (rowDef == vendorRD) {
            assertEquals(row.get(v_vx), ((Long)row.get(v_vid1)) * 100);
            assertEquals(row.get(v_vx), ((Long)row.get(v_vid2)) * 100);
        } else if (rowDef == customerRD) {
            assertEquals(row.get(c_vid1), ((Long) row.get(c_cid1)) / 10);
            assertEquals(row.get(c_vid2), ((Long) row.get(c_cid2)) / 10);
            assertEquals(row.get(c_cx), ((Long)row.get(c_cid1)) * 100);
            assertEquals(row.get(c_cx), ((Long)row.get(c_cid2)) * 100);
        } else if (rowDef == orderRD) {
            assertEquals(row.get(o_vid1), ((Long) row.get(o_cid1)) / 10);
            assertEquals(row.get(o_vid2), ((Long) row.get(o_cid2)) / 10);
            assertEquals(row.get(o_cid1), ((Long)row.get(o_oid1)) / 10);
            assertEquals(row.get(o_cid2), ((Long)row.get(o_oid2)) / 10);
            assertEquals(row.get(o_ox), ((Long)row.get(o_oid1)) * 100);
            assertEquals(row.get(o_ox), ((Long)row.get(o_oid2)) * 100);
        } else if (rowDef == itemRD) {
            assertEquals(row.get(i_vid1), ((Long) row.get(i_cid1)) / 10);
            assertEquals(row.get(i_vid2), ((Long) row.get(i_cid2)) / 10);
            assertEquals(row.get(i_cid1), ((Long)row.get(i_oid1)) / 10);
            assertEquals(row.get(i_cid2), ((Long)row.get(i_oid2)) / 10);
            assertEquals(row.get(i_oid1), ((Long)row.get(i_iid1)) / 10);
            assertEquals(row.get(i_oid2), ((Long)row.get(i_iid2)) / 10);
            assertEquals(row.get(i_ix), ((Long)row.get(i_iid1)) * 100);
            assertEquals(row.get(i_ix), ((Long)row.get(i_iid2)) * 100);
        } else {
            fail();
        }
    }

    protected void confirmColumns()
    {
        confirmColumn(vendorRD, v_vid1, "vid1");
        confirmColumn(vendorRD, v_vid2, "vid2");
        confirmColumn(vendorRD, v_vx, "vx");

        confirmColumn(customerRD, c_vid1, "vid1");
        confirmColumn(customerRD, c_vid2, "vid2");
        confirmColumn(customerRD, c_cid1, "cid1");
        confirmColumn(customerRD, c_cid2, "cid2");
        confirmColumn(customerRD, c_cx, "cx");

        confirmColumn(orderRD, o_vid1, "vid1");
        confirmColumn(orderRD, o_vid2, "vid2");
        confirmColumn(orderRD, o_cid1, "cid1");
        confirmColumn(orderRD, o_cid2, "cid2");
        confirmColumn(orderRD, o_oid1, "oid1");
        confirmColumn(orderRD, o_oid2, "oid2");
        confirmColumn(orderRD, o_ox, "ox");
        confirmColumn(orderRD, o_priority, "priority");
        confirmColumn(orderRD, o_when, "when");

        confirmColumn(itemRD, i_vid1, "vid1");
        confirmColumn(itemRD, i_vid2, "vid2");
        confirmColumn(itemRD, i_cid1, "cid1");
        confirmColumn(itemRD, i_cid2, "cid2");
        confirmColumn(itemRD, i_oid1, "oid1");
        confirmColumn(itemRD, i_oid2, "oid2");
        confirmColumn(itemRD, i_iid1, "iid1");
        confirmColumn(itemRD, i_iid2, "iid2");
        confirmColumn(itemRD, i_ix, "ix");
    }
}
