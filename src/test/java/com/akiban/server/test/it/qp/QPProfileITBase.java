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

package com.akiban.server.test.it.qp;

import com.akiban.ais.model.*;
import com.akiban.qp.persistitadapter.OperatorStore;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.persistitadapter.PersistitGroupRow;
import com.akiban.qp.persistitadapter.PersistitRowLimit;
import com.akiban.qp.operator.Bindings;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Limit;
import com.akiban.qp.operator.UndefBindings;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.api.dml.scan.NiceRow;
import com.akiban.server.api.dml.scan.ScanLimit;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.store.PersistitStore;
import com.akiban.server.store.Store;
import com.akiban.server.test.it.ITBase;
import com.akiban.server.types.ToObjectValueTarget;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QPProfileITBase extends ITBase
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
        coi = groupTable(customer);
        Store plainStore = store();
        final PersistitStore persistitStore;
        if (plainStore instanceof OperatorStore) {
            OperatorStore operatorStore = (OperatorStore) plainStore;
            persistitStore = operatorStore.getPersistitStore();
        }
        else {
            persistitStore = (PersistitStore) plainStore;
        }
        adapter = new PersistitAdapter(schema, persistitStore, null, session(), akServer());
    }

    protected void populateDB(int customers, int ordersPerCustomer, int itemsPerOrder)
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

    protected GroupTable groupTable(int userTableId)
    {
        RowDef userTableRowDef = rowDefCache().rowDef(userTableId);
        return userTableRowDef.table().getGroup().getGroupTable();
    }

    protected UserTable userTable(int userTableId)
    {
        RowDef userTableRowDef = rowDefCache().rowDef(userTableId);
        return userTableRowDef.userTable();
    }

    protected IndexRowType indexType(int userTableId, String... searchIndexColumnNamesArray)
    {
        UserTable userTable = userTable(userTableId);
        for (Index index : userTable.getIndexesIncludingInternal()) {
            List<String> indexColumnNames = new ArrayList<String>();
            for (IndexColumn indexColumn : index.getColumns()) {
                indexColumnNames.add(indexColumn.getColumn().getName());
            }
            List<String> searchIndexColumnNames = Arrays.asList(searchIndexColumnNamesArray);
            if (searchIndexColumnNames.equals(indexColumnNames)) {
                return schema.userTableRowType(userTable(userTableId)).indexRowType(index);
            }
        }
        return null;
    }

    protected ColumnSelector columnSelector(final Index index)
    {
        return new ColumnSelector()
        {
            @Override
            public boolean includesColumn(int columnPosition)
            {
                for (IndexColumn indexColumn : index.getColumns()) {
                    Column column = indexColumn.getColumn();
                    if (column.getPosition() == columnPosition) {
                        return true;
                    }
                }
                return false;
            }
        };
    }

    protected RowBase row(RowType rowType, Object... fields)
    {
        return new TestRow(rowType, fields);
    }

    protected RowBase row(int tableId, Object... values /* alternating field position and value */)
    {
        NiceRow niceRow = new NiceRow(tableId);
        int i = 0;
        while (i < values.length) {
            int position = (Integer) values[i++];
            Object value = values[i++];
            niceRow.put(position, value);
        }
        return PersistitGroupRow.newPersistitGroupRow(adapter, niceRow.toRowData());
    }

    protected void compareRows(RowBase[] expected, Cursor cursor)
    {
        compareRows(expected, cursor, NO_BINDINGS);
    }

    protected void compareRows(RowBase[] expected, Cursor cursor, Bindings bindings)
    {
        List<RowBase> actualRows = new ArrayList<RowBase>(); // So that result is viewable in debugger
        try {
            cursor.open(bindings);
            RowBase actualRow;
            while ((actualRow = cursor.next()) != null) {
                int count = actualRows.size();
                assertTrue(count < expected.length);
                if(!equal(expected[count], actualRow)) {
                    String expectedString = expected[count] == null ? "null" : expected[count].toString();
                    String actualString = actualRow == null ? "null" : actualRow.toString();
                    assertEquals(expectedString, actualString);
                }
                actualRows.add(actualRow);
            }
        } finally {
            cursor.close();
        }
        assertEquals(expected.length, actualRows.size());
    }

    protected void compareRenderedHKeys(String[] expected, Cursor cursor)
    {
        int count;
        try {
            cursor.open(NO_BINDINGS);
            count = 0;
            List<RowBase> actualRows = new ArrayList<RowBase>(); // So that result is viewable in debugger
            RowBase actualRow;
            while ((actualRow = cursor.next()) != null) {
                assertEquals(expected[count], actualRow.hKey().toString());
                count++;
                actualRows.add(actualRow);
            }
        } finally {
            cursor.close();
        }
        assertEquals(expected.length, count);
    }

    protected boolean equal(RowBase expected, RowBase actual)
    {
        boolean equal = expected.rowType().nFields() == actual.rowType().nFields();
        ToObjectValueTarget target = new ToObjectValueTarget();
        for (int i = 0; equal && i < actual.rowType().nFields(); i++) {
            Object expectedField = target.convertFromSource(expected.eval(i));
            Object actualField = target.convertFromSource(actual.eval(i));
            equal =
                expectedField == actualField || // handles case in which both are null
                expectedField != null && actualField != null && expectedField.equals(actualField);
        }
        return equal;
    }

    protected static final Bindings NO_BINDINGS = UndefBindings.only();
    protected static final Limit NO_LIMIT = new PersistitRowLimit(ScanLimit.NONE);

    protected int customer;
    protected int order;
    protected int item;
    protected int address;
    protected RowType customerRowType;
    protected RowType orderRowType;
    protected RowType itemRowType;
    protected RowType addressRowType;
    protected IndexRowType customerCidIndexRowType;
    protected IndexRowType customerNameIndexRowType;
    protected IndexRowType orderSalesmanIndexRowType;
    protected IndexRowType itemOidIndexRowType;
    protected IndexRowType itemIidIndexRowType;
    protected IndexRowType addressAddressIndexRowType;
    protected GroupTable coi;
    protected Schema schema;
    PersistitAdapter adapter;
}
