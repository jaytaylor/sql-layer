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

package com.akiban.server.test.pt.gi;

import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.api.dml.scan.NiceRow;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.test.pt.PTBase;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ToObjectValueTarget;
import com.akiban.util.tap.Tap;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.akiban.qp.operator.API.*;

public class GIUpdateProfilePT extends PTBase
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
        coi = groupTable(customer);
        String groupName = coi.getGroup().getName();
        createGroupIndex(groupName, "name_salesman", "customer.name, order.salesman");
        createGroupIndex(groupName, "name_address", "customer.name, address.address");
        schema = new Schema(rowDefCache().ais());
        customerRowType = schema.userTableRowType(userTable(customer));
        orderRowType = schema.userTableRowType(userTable(order));
        itemRowType = schema.userTableRowType(userTable(item));
        addressRowType = schema.userTableRowType(userTable(address));
        customerNameIndexRowType = indexType(customer, "name");
        orderSalesmanIndexRowType = indexType(order, "salesman");
        itemOidIndexRowType = indexType(item, "oid");
        itemIidIndexRowType = indexType(item, "iid");
        customerCidIndexRowType = indexType(customer, "cid");
        addressAddressIndexRowType = indexType(address, "address");
        queryContext = queryContext(persistitAdapter(schema));
    }

    @Test
    public void profileUpdate()
    {
        final int CUSTOMERS = 10;
        final int ORDERS_PER_CUSTOMER = 5;
        final int ITEMS_PER_ORDER = 2;
        populateDB(CUSTOMERS, ORDERS_PER_CUSTOMER, ITEMS_PER_ORDER);
        Operator scan = filter_Default(groupScan_Default(coi), Collections.singleton(customerRowType));
        ToObjectValueTarget target = new ToObjectValueTarget();
        target.expectType(AkType.VARCHAR);
        Tap.setEnabled(".*", true);
        for (int s = 0; s < 100000000 ; s++) {
            Cursor cursor = cursor(scan, queryContext);
            cursor.open();
            Row row;
            RowDef customerRowDef = customerRowType.userTable().rowDef();
            while ((row = cursor.next()) != null) {
                NiceRow oldRow = new NiceRow(customer, customerRowDef);
                NiceRow newRow  = new NiceRow(customer, customerRowDef);
                long cid = row.eval(0).getInt();
                String name = row.eval(1).getString();
                oldRow.put(0, cid);
                oldRow.put(1, name);
                newRow.put(0, cid);
                newRow.put(1, name + ":CHANGED");
                dml().updateRow(session(), oldRow, newRow, null);
                dml().updateRow(session(), newRow, oldRow, null);
            }
        }
    }

    private void populateDB(int customers, int ordersPerCustomer, int itemsPerOrder)
    {
        long cid = 0;
        long oid = 0;
        long iid = 0;
        for (int c = 0; c < customers; c++) {
            dml().writeRow(session(), createNewRow(customer, cid, String.format("customer %s", cid)));
            for (int o = 0; o < ordersPerCustomer; o++) {
                dml().writeRow(session(), createNewRow(order, oid, cid, String.format("salesman %s", oid)));
                for (int i = 0; i < itemsPerOrder; i++) {
                    dml().writeRow(session(), createNewRow(item, iid, oid));
                    iid++;
                }
                oid++;
            }
            cid++;
        }
    }

    private GroupTable groupTable(int userTableId)
    {
        RowDef userTableRowDef = rowDefCache().rowDef(userTableId);
        return userTableRowDef.table().getGroup().getGroupTable();
    }

    private UserTable userTable(int userTableId)
    {
        RowDef userTableRowDef = rowDefCache().rowDef(userTableId);
        return userTableRowDef.userTable();
    }

    private IndexRowType indexType(int userTableId, String... searchIndexColumnNamesArray)
    {
        UserTable userTable = userTable(userTableId);
        for (Index index : userTable.getIndexesIncludingInternal()) {
            List<String> indexColumnNames = new ArrayList<String>();
            for (IndexColumn indexColumn : index.getKeyColumns()) {
                indexColumnNames.add(indexColumn.getColumn().getName());
            }
            List<String> searchIndexColumnNames = Arrays.asList(searchIndexColumnNamesArray);
            if (searchIndexColumnNames.equals(indexColumnNames)) {
                return schema.userTableRowType(userTable(userTableId)).indexRowType(index);
            }
        }
        return null;
    }

    private int customer;
    private int order;
    private int item;
    private int address;
    private GroupTable coi;
    private Schema schema;
    private RowType customerRowType;
    private RowType orderRowType;
    private RowType itemRowType;
    private RowType addressRowType;
    private IndexRowType customerNameIndexRowType;
    private IndexRowType orderSalesmanIndexRowType;
    private IndexRowType itemOidIndexRowType;
    private IndexRowType itemIidIndexRowType;
    private IndexRowType customerCidIndexRowType;
    private IndexRowType addressAddressIndexRowType;
    private QueryContext queryContext;
}
