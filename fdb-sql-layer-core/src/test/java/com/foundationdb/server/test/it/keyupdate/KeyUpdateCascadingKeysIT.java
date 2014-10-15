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

import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.rowdata.RowDef;
import org.junit.Test;

import java.util.List;

import static com.foundationdb.server.test.it.keyupdate.Schema.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

// Like KeyUpdateIT, but with cascading keys

public class KeyUpdateCascadingKeysIT extends KeyUpdateBase
{
    @Test
    public void testItemFKUpdate() throws Exception
    {
        // Set item.oid = o for item 1222
        TestRow originalItem = testStore.find(new HKey(vendorRD, 1L, customerRD, 12L, orderRD, 122L, itemRD, 1222L));
        TestRow updatedItem = copyRow(originalItem);
        updateRow(updatedItem, i_oid, 0L);
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
        TestRow originalItem = testStore.find(new HKey(vendorRD, 1L, customerRD, 12L, orderRD, 122L, itemRD, 1222L));
        TestRow newItem = copyRow(originalItem);
        updateRow(newItem, i_iid, 0L);
        startMonitoringHKeyPropagation();
        dbUpdate(originalItem, newItem);
        checkHKeyPropagation(0, 0);
        checkDB();
        // Revert change
        startMonitoringHKeyPropagation();
        dbUpdate(newItem, originalItem);
        checkHKeyPropagation(0, 0);
        checkDB();
        checkInitialState();
    }

    @Test
    public void testItemPKUpdateCreatingDuplicate() throws Exception
    {
        // Set item.iid = 1223 for item 1222
        TestRow originalItem = testStore.find(new HKey(vendorRD, 1L, customerRD, 12L, orderRD, 122L, itemRD, 1222L));
        TestRow updatedItem = copyRow(originalItem);
        updateRow(updatedItem, i_iid, 1223);
        try {
            dbUpdate(originalItem, updatedItem);
            fail();
        } catch (InvalidOperationException e) {
            assertEquals(e.getCode(), ErrorCode.DUPLICATE_KEY);
        }
        checkDB();
    }

    @Test
    public void testOrderFKUpdate() throws Exception
    {
        // Set order.cid = 0 for order 222
        TestRow originalOrder = testStore.find(new HKey(vendorRD, 2L, customerRD, 22L, orderRD, 222L));
        TestRow updatedOrder = copyRow(originalOrder);
        updateRow(updatedOrder, o_cid, 0L);
        startMonitoringHKeyPropagation();
        dbUpdate(originalOrder, updatedOrder);
        checkHKeyPropagation(2, 0);
        checkDB();
        // Revert change
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
        TestRow originalOrder = testStore.find(new HKey(vendorRD, 2L, customerRD, 22L, orderRD, 222L));
        TestRow updatedOrder = copyRow(originalOrder);
        updateRow(updatedOrder, o_oid, 0L);
        startMonitoringHKeyPropagation();
        dbUpdate(originalOrder, updatedOrder);
        checkHKeyPropagation(2, 0);
        checkDB();
        // Revert change
        startMonitoringHKeyPropagation();
        dbUpdate(updatedOrder, originalOrder);
        checkHKeyPropagation(2, 0);
        checkDB();
        checkInitialState();
    }

    @Test
    public void testOrderPKUpdateCreatingDuplicate() throws Exception
    {
        // Set order.oid = 221 for order 222
        TestRow originalOrder = testStore.find(new HKey(vendorRD, 2L, customerRD, 22L, orderRD, 222L));
        TestRow updatedOrder = copyRow(originalOrder);
        updateRow(updatedOrder, o_oid, 221);
        try {
            dbUpdate(originalOrder, updatedOrder);
            fail();
        } catch (InvalidOperationException e) {
            assertEquals(e.getCode(), ErrorCode.DUPLICATE_KEY);
        }
        checkDB();
    }

    @Test
    public void testCustomerFKUpdate() throws Exception
    {
        // Set order.vid = 0 for customer 13
        TestRow originalCustomer = testStore.find(new HKey(vendorRD, 1L, customerRD, 13L));
        TestRow updatedCustomer = copyRow(originalCustomer);
        updateRow(updatedCustomer, c_vid, 0L);
        startMonitoringHKeyPropagation();
        dbUpdate(originalCustomer, updatedCustomer);
        checkHKeyPropagation(2, 0);
        checkDB();
        // Revert change
        startMonitoringHKeyPropagation();
        dbUpdate(updatedCustomer, originalCustomer);
        checkHKeyPropagation(2, 0);
        checkDB();
        checkInitialState();
    }

    @Test
    public void testCustomerPKUpdate() throws Exception
    {
        // Set customer.cid = 0 for customer 22
        TestRow originalCustomer = testStore.find(new HKey(vendorRD, 2L, customerRD, 22L));
        TestRow updatedCustomer = copyRow(originalCustomer);
        updateRow(updatedCustomer, c_cid, 0L);
        startMonitoringHKeyPropagation();
        dbUpdate(originalCustomer, updatedCustomer);
        checkHKeyPropagation(2, 0);
        checkDB();
        // Revert change
        startMonitoringHKeyPropagation();
        dbUpdate(updatedCustomer, originalCustomer);
        checkHKeyPropagation(2, 0);
        checkDB();
        checkInitialState();
    }

    @Test
    public void testCustomerPKUpdateCreatingDuplicate() throws Exception
    {
        // Set customer.vid = 11 for customer 23
        TestRow oldCustomerRow = testStore.find(new HKey(vendorRD, 2L, customerRD, 23L));
        TestRow newCustomerRow = copyRow(oldCustomerRow);
        updateRow(newCustomerRow, c_vid, 1);
        updateRow(newCustomerRow, c_cid, 11);
        try {
            dbUpdate(oldCustomerRow, newCustomerRow);
            fail();
        } catch (InvalidOperationException e) {
            assertEquals(e.getCode(), ErrorCode.DUPLICATE_KEY);
        }
        checkDB();
    }

    @Test
    public void testVendorPKUpdate() throws Exception
    {
        // Set vendor.vid = 0 for vendor 1
        TestRow originalVendor = testStore.find(new HKey(vendorRD, 1L));
        TestRow updatedVendor = copyRow(originalVendor);
        updateRow(updatedVendor, v_vid, 0L, null);
        startMonitoringHKeyPropagation();
        dbUpdate(originalVendor, updatedVendor);
        checkHKeyPropagation(2, 0);
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
        TestRow originalVendorRow = testStore.find(new HKey(vendorRD, 1L));
        TestRow updatedVendorRow = copyRow(originalVendorRow);
        updateRow(updatedVendorRow, v_vid, 2, null);
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
        TestRow itemRow = testStore.find(new HKey(vendorRD, 2L, customerRD, 22L, orderRD, 222L, itemRD, 2222L));
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
        TestRow orderRow = testStore.find(new HKey(vendorRD, 2L, customerRD, 22L, orderRD, 222L));
        startMonitoringHKeyPropagation();
        dbDelete(orderRow);
        checkHKeyPropagation(1, 3);
        checkDB();
        // Revert change
        startMonitoringHKeyPropagation();
        dbInsert(orderRow);
        checkHKeyPropagation(1, 3);
        checkDB();
        checkInitialState();
    }

    @Test
    public void testCustomerDelete() throws Exception
    {
        TestRow customerRow = testStore.find(new HKey(vendorRD, 2L, customerRD, 22L));
        startMonitoringHKeyPropagation();
        dbDelete(customerRow);
        checkHKeyPropagation(1, 12);
        checkDB();
        // Revert change
        startMonitoringHKeyPropagation();
        dbInsert(customerRow);
        checkHKeyPropagation(1, 12);
        checkDB();
        checkInitialState();
    }
    
    @Test
    public void testVendorDelete() throws Exception
    {
        TestRow vendorRow = testStore.find(new HKey(vendorRD, 2L));
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
                               "vid bigint not null",
                               "vx bigint",
                               "primary key(vid)");
        v_vid = 0;
        v_vx = 1;
        // customer
        customerId = createTable("coi", "customer",
                                 "vid bigint not null",
                                 "cid bigint not null",
                                 "cx bigint",
                                 "primary key(vid, cid)",
                                 "grouping foreign key (vid) references vendor(vid)");
        c_vid = 0;
        c_cid = 1;
        c_cx = 2;
        // order
        orderId = createTable("coi", "order",
                              "vid bigint not null",
                              "cid bigint not null",
                              "oid bigint not null",
                              "ox bigint",
                              "priority bigint",
                              "when bigint",
                              "primary key(vid, cid, oid)",
                              "unique(when)",
                              "grouping foreign key (vid, cid) references customer(vid, cid)");
        createIndex("coi", "order", "priority", "priority");
        o_vid = 0;
        o_cid = 1;
        o_oid = 2;
        o_ox = 3;
        o_priority = 4;
        o_when = 5;
        // item
        itemId = createTable("coi", "item",
                             "vid bigint not null",
                             "cid bigint not null",
                             "oid bigint not null",
                             "iid bigint not null",
                             "ix bigint",
                             "primary key(vid, cid, oid, iid)",
                             "grouping foreign key (vid, cid, oid) references \"order\"(vid, cid, oid)");
        i_vid = 0;
        i_cid = 1;
        i_oid = 2;
        i_iid = 3;
        i_ix = 4;
        vendorRD = getRowDef(vendorId);
        customerRD = getRowDef(customerId);
        orderRD = getRowDef(orderId);
        itemRD = getRowDef(itemId);
        // group
        group = customerRD.getGroup();
    }

    @Override
    protected List<List<Object>> customerPKIndex(List<TreeRecord> records)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected List<List<Object>> vendorPKIndex(List<TreeRecord> records)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected List<List<Object>> orderPKIndex(List<TreeRecord> records)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected List<List<Object>> itemPKIndex(List<TreeRecord> records)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected List<List<Object>> orderPriorityIndex(List<TreeRecord> records)
    {
        return indexFromRecords(records, orderRD, o_priority, o_vid, o_cid, o_oid);
    }

    @Override
    protected List<List<Object>> orderWhenIndex(List<TreeRecord> records)
    {
        return indexFromRecords(records, orderRD, o_when, o_vid, o_cid, o_oid);
    }

    @Override
    protected void populateTables() throws Exception
    {
        // Vendor 1
        dbInsert(row(vendorRD,   1, 100));
        dbInsert(row(customerRD, 1, 11, 1100));
        dbInsert(row(orderRD,    1, 11, 111, 11100, 81, 9001));
        dbInsert(row(itemRD,     1, 11, 111, 1111, 111100));
        dbInsert(row(itemRD,     1, 11, 111, 1112, 111200));
        dbInsert(row(itemRD,     1, 11, 111, 1113, 111300));
        dbInsert(row(orderRD,    1, 11, 112, 11200, 83, 9002));
        dbInsert(row(itemRD,     1, 11, 112, 1121, 112100));
        dbInsert(row(itemRD,     1, 11, 112, 1122, 112200));
        dbInsert(row(itemRD,     1, 11, 112, 1123, 112300));
        dbInsert(row(orderRD,    1, 11, 113, 11300, 81, 9003));
        dbInsert(row(itemRD,     1, 11, 113, 1131, 113100));
        dbInsert(row(itemRD,     1, 11, 113, 1132, 113200));
        dbInsert(row(itemRD,     1, 11, 113, 1133, 113300));

        dbInsert(row(customerRD, 1, 12, 1200));
        dbInsert(row(orderRD,    1, 12, 121, 12100, 83, 9004));
        dbInsert(row(itemRD,     1, 12, 121, 1211, 121100));
        dbInsert(row(itemRD,     1, 12, 121, 1212, 121200));
        dbInsert(row(itemRD,     1, 12, 121, 1213, 121300));
        dbInsert(row(orderRD,    1, 12, 122, 12200, 81, 9005));
        dbInsert(row(itemRD,     1, 12, 122, 1221, 122100));
        dbInsert(row(itemRD,     1, 12, 122, 1222, 122200));
        dbInsert(row(itemRD,     1, 12, 122, 1223, 122300));
        dbInsert(row(orderRD,    1, 12, 123, 12300, 82, 9006));
        dbInsert(row(itemRD,     1, 12, 123, 1231, 123100));
        dbInsert(row(itemRD,     1, 12, 123, 1232, 123200));
        dbInsert(row(itemRD,     1, 12, 123, 1233, 123300));

        dbInsert(row(customerRD, 1, 13, 1300));
        dbInsert(row(orderRD,    1, 13, 131, 13100, 81, 9007));
        dbInsert(row(itemRD,     1, 13, 131, 1311, 131100));
        dbInsert(row(itemRD,     1, 13, 131, 1312, 131200));
        dbInsert(row(itemRD,     1, 13, 131, 1313, 131300));
        dbInsert(row(orderRD,    1, 13, 132, 13200, 82, 9008));
        dbInsert(row(itemRD,     1, 13, 132, 1321, 132100));
        dbInsert(row(itemRD,     1, 13, 132, 1322, 132200));
        dbInsert(row(itemRD,     1, 13, 132, 1323, 132300));
        dbInsert(row(orderRD,    1, 13, 133, 13300, 83, 9009));
        dbInsert(row(itemRD,     1, 13, 133, 1331, 133100));
        dbInsert(row(itemRD,     1, 13, 133, 1332, 133200));
        dbInsert(row(itemRD,     1, 13, 133, 1333, 133300));

        // Vendor 2
        dbInsert(row(vendorRD,   2, 200));
        dbInsert(row(customerRD, 2, 21, 2100));
        dbInsert(row(orderRD,    2, 21, 211, 21100, 81, 9010));
        dbInsert(row(itemRD,     2, 21, 211, 2111, 211100));
        dbInsert(row(itemRD,     2, 21, 211, 2112, 211200));
        dbInsert(row(itemRD,     2, 21, 211, 2113, 211300));
        dbInsert(row(orderRD,    2, 21, 212, 21200, 83, 9011));
        dbInsert(row(itemRD,     2, 21, 212, 2121, 212100));
        dbInsert(row(itemRD,     2, 21, 212, 2122, 212200));
        dbInsert(row(itemRD,     2, 21, 212, 2123, 212300));
        dbInsert(row(orderRD,    2, 21, 213, 21300, 81, 9012));
        dbInsert(row(itemRD,     2, 21, 213, 2131, 213100));
        dbInsert(row(itemRD,     2, 21, 213, 2132, 213200));
        dbInsert(row(itemRD,     2, 21, 213, 2133, 213300));

        dbInsert(row(customerRD, 2, 22, 2200));
        dbInsert(row(orderRD,    2, 22, 221, 22100, 83, 9013));
        dbInsert(row(itemRD,     2, 22, 221, 2211, 221100));
        dbInsert(row(itemRD,     2, 22, 221, 2212, 221200));
        dbInsert(row(itemRD,     2, 22, 221, 2213, 221300));
        dbInsert(row(orderRD,    2, 22, 222, 22200, 81, 9014));
        dbInsert(row(itemRD,     2, 22, 222, 2221, 222100));
        dbInsert(row(itemRD,     2, 22, 222, 2222, 222200));
        dbInsert(row(itemRD,     2, 22, 222, 2223, 222300));
        dbInsert(row(orderRD,    2, 22, 223, 22300, 82, 9015));
        dbInsert(row(itemRD,     2, 22, 223, 2231, 223100));
        dbInsert(row(itemRD,     2, 22, 223, 2232, 223200));
        dbInsert(row(itemRD,     2, 22, 223, 2233, 223300));

        dbInsert(row(customerRD, 2, 23, 2300));
        dbInsert(row(orderRD,    2, 23, 231, 23100, 81, 9016));
        dbInsert(row(itemRD,     2, 23, 231, 2311, 231100));
        dbInsert(row(itemRD,     2, 23, 231, 2312, 231200));
        dbInsert(row(itemRD,     2, 23, 231, 2313, 231300));
        dbInsert(row(orderRD,    2, 23, 232, 23200, 82, 9017));
        dbInsert(row(itemRD,     2, 23, 232, 2321, 232100));
        dbInsert(row(itemRD,     2, 23, 232, 2322, 232200));
        dbInsert(row(itemRD,     2, 23, 232, 2323, 232300));
        dbInsert(row(orderRD,    2, 23, 233, 23300, 83, 9018));
        dbInsert(row(itemRD,     2, 23, 233, 2331, 233100));
        dbInsert(row(itemRD,     2, 23, 233, 2332, 233200));
        dbInsert(row(itemRD,     2, 23, 233, 2333, 233300));
    }

    @Override
    protected HKey hKey(TestRow row)
    {
        HKey hKey = null;
        RowDef rowDef = row.getRowDef();
        if (rowDef == vendorRD) {
            hKey = new HKey(vendorRD, row.get(v_vid));
        } else if (rowDef == customerRD) {
            hKey = new HKey(vendorRD, row.get(c_vid),
                            customerRD, row.get(c_cid));
        } else if (rowDef == orderRD) {
            hKey = new HKey(vendorRD, row.get(o_vid),
                            customerRD, row.get(o_cid),
                            orderRD, row.get(o_oid));
        } else if (rowDef == itemRD) {
            hKey = new HKey(vendorRD, row.get(i_vid),
                            customerRD, row.get(i_cid),
                            orderRD, row.get(i_oid),
                            itemRD, row.get(i_iid));
        } else {
            fail();
        }
        return hKey;
    }

    @Override
    protected HKey hKey(TestRow row, TestRow parent) {
        return hKey(row);
    }

    @Override
    protected HKey hKey(TestRow row, TestRow parent, TestRow grandparent) {
        return hKey(row);
    }

    @Override
    protected boolean checkChildPKs() {
        return false;
    }

    protected void confirmColumns()
    {
        confirmColumn(vendorRD, v_vid, "vid");
        confirmColumn(vendorRD, v_vx, "vx");

        confirmColumn(customerRD, c_vid, "vid");
        confirmColumn(customerRD, c_cid, "cid");
        confirmColumn(customerRD, c_cx, "cx");

        confirmColumn(orderRD, o_vid, "vid");
        confirmColumn(orderRD, o_cid, "cid");
        confirmColumn(orderRD, o_oid, "oid");
        confirmColumn(orderRD, o_ox, "ox");
        confirmColumn(orderRD, o_priority, "priority");
        confirmColumn(orderRD, o_when, "when");

        confirmColumn(itemRD, i_vid, "vid");
        confirmColumn(itemRD, i_cid, "cid");
        confirmColumn(itemRD, i_oid, "oid");
        confirmColumn(itemRD, i_iid, "iid");
        confirmColumn(itemRD, i_ix, "ix");
    }
}
