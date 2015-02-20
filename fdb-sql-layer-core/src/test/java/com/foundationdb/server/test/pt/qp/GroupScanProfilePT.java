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

package com.foundationdb.server.test.pt.qp;

import com.foundationdb.ais.model.Group;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.error.InvalidOperationException;

import org.junit.Before;
import org.junit.Test;

import static com.foundationdb.qp.operator.API.cursor;
import static com.foundationdb.qp.operator.API.groupScan_Default;

public class GroupScanProfilePT extends QPProfilePTBase
{
    @Before
    public void before() throws InvalidOperationException
    {
        customer = createTable(
            "schema", "customer",
            "cid int not null key",
            "name varchar(20)," +
            "index(name)");
        order = createTable(
            "schema", "order",
            "oid int not null key",
            "cid int",
            "salesman varchar(20)",
            "constraint __akiban_oc foreign key __akiban_oc(cid) references customer(cid)",
            "index(salesman)");
        item = createTable(
            "schema", "item",
            "iid int not null key",
            "oid int",
            "constraint __akiban_io foreign key __akiban_io(oid) references order(oid)");
        address = createTable(
            "schema", "address",
            "aid int not null key",
            "cid int",
            "address varchar(100)",
            "constraint __akiban_ac foreign key __akiban_ac(cid) references customer(cid)",
            "index(address)");
        schema = SchemaCache.globalSchema(ais());
        customerRowType = schema.tableRowType(table(customer));
        orderRowType = schema.tableRowType(table(order));
        itemRowType = schema.tableRowType(table(item));
        addressRowType = schema.tableRowType(table(address));
        customerNameIndexRowType = indexType(customer, "name");
        orderSalesmanIndexRowType = indexType(order, "salesman");
        itemOidIndexRowType = indexType(item, "oid");
        itemIidIndexRowType = indexType(item, "iid");
        customerCidIndexRowType = indexType(customer, "cid");
        addressAddressIndexRowType = indexType(address, "address");
        coi = group(customer);
        adapter = newStoreAdapter();
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
    }

    protected void populateDB(int customers, int ordersPerCustomer, int itemsPerOrder)
    {
        long cid = 0;
        long oid = 0;
        long iid = 0;
        for (int c = 0; c < customers; c++) {
            writeRow(row(customer, cid, String.format("customer %s", cid)));
            for (int o = 0; o < ordersPerCustomer; o++) {
                writeRow(row(order, oid, cid, String.format("salesman %s", oid)));
                for (int i = 0; i < itemsPerOrder; i++) {
                    writeRow(row(item, iid, oid));
                    iid++;
                }
                oid++;
            }
            cid++;
        }
    }

    @Test
    public void profileGroupScan()
    {
        final int SCANS = 1000;
        final int CUSTOMERS = 1000;
        final int ORDERS_PER_CUSTOMER = 5;
        final int ITEMS_PER_ORDER = 2;
        populateDB(CUSTOMERS, ORDERS_PER_CUSTOMER, ITEMS_PER_ORDER);
        long start = System.nanoTime();
        Operator plan = groupScan_Default(coi);
        for (int s = 0; s < SCANS; s++) {
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
            while (cursor.next() != null) {
            }
            cursor.closeTopLevel();
        }
        long end = System.nanoTime();
        double sec = (end - start) / (1000.0 * 1000 * 1000);
        System.out.println(String.format("scans: %s, db: %s/%s/%s, time: %s",
                                         SCANS, CUSTOMERS, ORDERS_PER_CUSTOMER, ITEMS_PER_ORDER, sec));
    }

    protected int customer;
    protected int order;
    protected int item;
    protected int address;
    protected RowType      customerRowType;
    protected RowType orderRowType;
    protected RowType itemRowType;
    protected RowType addressRowType;
    protected IndexRowType customerCidIndexRowType;
    protected IndexRowType customerNameIndexRowType;
    protected IndexRowType orderSalesmanIndexRowType;
    protected IndexRowType itemOidIndexRowType;
    protected IndexRowType itemIidIndexRowType;
    protected IndexRowType addressAddressIndexRowType;
    protected Group coi;
}
