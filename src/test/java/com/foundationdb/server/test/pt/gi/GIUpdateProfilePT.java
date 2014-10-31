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

package com.foundationdb.server.test.pt.gi;

import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.api.dml.scan.NiceRow;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.server.test.pt.PTBase;
import com.foundationdb.util.tap.Tap;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.foundationdb.qp.operator.API.*;

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
        coi = group(customer);
        TableName groupName = coi.getName();
        createLeftGroupIndex(groupName, "name_salesman", "customer.name", "order.salesman");
        createLeftGroupIndex(groupName, "name_address", "customer.name", "address.address");
        schema = new Schema(ais());
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
        queryContext = queryContext(newStoreAdapter(schema));
        queryBindings = queryContext.createBindings();
    }

    @Test
    public void profileUpdate()
    {
        final int CUSTOMERS = 10;
        final int ORDERS_PER_CUSTOMER = 5;
        final int ITEMS_PER_ORDER = 2;
        populateDB(CUSTOMERS, ORDERS_PER_CUSTOMER, ITEMS_PER_ORDER);
        Operator scan = filter_Default(groupScan_Default(coi), Collections.singleton(customerRowType));
        Tap.setEnabled(".*", true);
        for (int s = 0; s < 100000000 ; s++) {
            Cursor cursor = cursor(scan, queryContext, queryBindings);
            cursor.openTopLevel();
            Row row;
            RowDef customerRowDef = customerRowType.table().rowDef();
            while ((row = cursor.next()) != null) {
                NiceRow oldRow = new NiceRow(customer, customerRowDef);
                NiceRow newRow  = new NiceRow(customer, customerRowDef);
                long cid = getLong(row, 0);
                String name = row.value(1).getString();
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
            writeRow(customer, cid, String.format("customer %s", cid));
            for (int o = 0; o < ordersPerCustomer; o++) {
                writeRow(order, oid, cid, String.format("salesman %s", oid));
                for (int i = 0; i < itemsPerOrder; i++) {
                    writeRow(item, iid, oid);
                    iid++;
                }
                oid++;
            }
            cid++;
        }
    }

    private Group group(int tableId)
    {
        return getRowDef(tableId).table().getGroup();
    }

    private Table table(int tableId)
    {
        return getRowDef(tableId).table();
    }

    private IndexRowType indexType(int tableId, String... searchIndexColumnNamesArray)
    {
        Table table = table(tableId);
        for (Index index : table.getIndexesIncludingInternal()) {
            List<String> indexColumnNames = new ArrayList<>();
            for (IndexColumn indexColumn : index.getKeyColumns()) {
                indexColumnNames.add(indexColumn.getColumn().getName());
            }
            List<String> searchIndexColumnNames = Arrays.asList(searchIndexColumnNamesArray);
            if (searchIndexColumnNames.equals(indexColumnNames)) {
                return schema.tableRowType(table(tableId)).indexRowType(index);
            }
        }
        return null;
    }

    private int customer;
    private int order;
    private int item;
    private int address;
    private Group coi;
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
    private QueryBindings queryBindings;
}
