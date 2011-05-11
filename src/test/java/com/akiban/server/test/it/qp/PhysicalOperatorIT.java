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
import com.akiban.qp.expression.Expression;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.persistitadapter.OperatorStore;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.persistitadapter.PersistitGroupRow;
import com.akiban.qp.physicaloperator.Bindings;
import com.akiban.qp.physicaloperator.Cursor;
import com.akiban.qp.physicaloperator.PhysicalOperator;
import com.akiban.qp.physicaloperator.UndefBindings;
import com.akiban.qp.physicaloperator.UpdateLambda;
import com.akiban.qp.physicaloperator.Update_Default;
import com.akiban.qp.row.OverlayingRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.RowDef;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.NiceRow;
import com.akiban.server.store.PersistitStore;
import com.akiban.server.store.Store;
import com.akiban.server.test.it.ITBase;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.akiban.qp.expression.API.*;
import static com.akiban.qp.physicaloperator.API.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class PhysicalOperatorIT extends ITBase
{

    private static final Bindings UNDEF_BINDINGS = UndefBindings.only();

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
        schema = new Schema(rowDefCache().ais());
        customerRowType = schema.userTableRowType(userTable(customer));
        orderRowType = schema.userTableRowType(userTable(order));
        itemRowType = schema.userTableRowType(userTable(item));
        customerNameIndexRowType = schema.indexRowType(index(customer, "name"));
        orderSalesmanIndexRowType = schema.indexRowType(index(order, "salesman"));
        coi = groupTable(customer);
        db = new NewRow[]{createNewRow(customer, 1L, "xyz"),
                          createNewRow(customer, 2L, "abc"),
                          createNewRow(order, 11L, 1L, "ori"),
                          createNewRow(order, 12L, 1L, "david"),
                          createNewRow(order, 21L, 2L, "tom"),
                          createNewRow(order, 22L, 2L, "jack"),
                          createNewRow(item, 111L, 11L),
                          createNewRow(item, 112L, 11L),
                          createNewRow(item, 121L, 12L),
                          createNewRow(item, 122L, 12L),
                          createNewRow(item, 211L, 21L),
                          createNewRow(item, 212L, 21L),
                          createNewRow(item, 221L, 22L),
                          createNewRow(item, 222L, 22L)};
        writeRows(db);
        Store plainStore = store();
        final PersistitStore persistitStore;
        if (plainStore instanceof OperatorStore) {
            OperatorStore operatorStore = (OperatorStore) plainStore;
            persistitStore = operatorStore.getPersistitStore();
        }
        else {
            persistitStore = (PersistitStore) plainStore;
        }
        adapter = new PersistitAdapter(schema, persistitStore, session());
    }

    @Test
    public void basicUpdate() throws Exception {
        adapter.setTransactional(false);

        UpdateLambda updateLambda = new UpdateLambda() {
            @Override
            public boolean rowIsApplicable(Row row) {
                return row.rowType().equals(customerRowType);
            }

            @Override
            public Row applyUpdate(Row original, Bindings bindings) {
                String name = (String) original.field(1, bindings); // TODO eventually use Expression for this
                name = name.toUpperCase();
                name = name + name;
                return new OverlayingRow(original).overlay(1, name);
            }
        };

        PhysicalOperator groupScan = groupScan_Default(coi);
        PhysicalOperator updateOperator = new Update_Default(groupScan, updateLambda);
        Cursor updateCursor = cursor(updateOperator, adapter);
        int nexts = 0;
        updateCursor.open(UNDEF_BINDINGS);
        while (updateCursor.next()) {
            ++nexts;
        }
        updateCursor.close();
        adapter.commitAllTransactions();
        assertEquals("invocations of next()", db.length, nexts);

        Cursor executable = cursor(groupScan, adapter);
        RowBase[] expected = new RowBase[]{
                row(customerRowType, 1L, "XYZXYZ"),
                row(orderRowType, 11L, 1L, "ori"),
                row(itemRowType, 111L, 11L),
                row(itemRowType, 112L, 11L),
                row(orderRowType, 12L, 1L, "david"),
                row(itemRowType, 121L, 12L),
                row(itemRowType, 122L, 12L),
                row(customerRowType, 2L, "ABCABC"),
                row(orderRowType, 21L, 2L, "tom"),
                row(itemRowType, 211L, 21L),
                row(itemRowType, 212L, 21L),
                row(orderRowType, 22L, 2L, "jack"),
                row(itemRowType, 221L, 22L),
                row(itemRowType, 222L, 22L)
        };
        compareRows(expected, executable);
    }

    @Test
    public void testGroupScan() throws Exception
    {
        PhysicalOperator groupScan = groupScan_Default(coi);
        Cursor executable = cursor(groupScan, adapter);
        RowBase[] expected = new RowBase[]{row(customerRowType, 1L, "xyz"),
                                   row(orderRowType, 11L, 1L, "ori"),
                                   row(itemRowType, 111L, 11L),
                                   row(itemRowType, 112L, 11L),
                                   row(orderRowType, 12L, 1L, "david"),
                                   row(itemRowType, 121L, 12L),
                                   row(itemRowType, 122L, 12L),
                                   row(customerRowType, 2L, "abc"),
                                   row(orderRowType, 21L, 2L, "tom"),
                                   row(itemRowType, 211L, 21L),
                                   row(itemRowType, 212L, 21L),
                                   row(orderRowType, 22L, 2L, "jack"),
                                   row(itemRowType, 221L, 22L),
                                   row(itemRowType, 222L, 22L)
        };
        compareRows(expected, executable);
    }

    @Test
    public void testSelect()
    {
        PhysicalOperator groupScan = groupScan_Default(coi);
        Expression cidEq2 = compare(field(0), EQ, literal(2L));
        PhysicalOperator select = select_HKeyOrdered(groupScan, customerRowType, cidEq2);
        RowBase[] expected = new RowBase[]{row(customerRowType, 2L, "abc"),
                                   row(orderRowType, 21L, 2L, "tom"),
                                   row(itemRowType, 211L, 21L),
                                   row(itemRowType, 212L, 21L),
                                   row(orderRowType, 22L, 2L, "jack"),
                                   row(itemRowType, 221L, 22L),
                                   row(itemRowType, 222L, 22L)};
        compareRows(expected, cursor(select, adapter));
    }

    @Test
    public void testFlatten()
    {
        PhysicalOperator groupScan = groupScan_Default(coi);
        PhysicalOperator flatten = flatten_HKeyOrdered(groupScan, customerRowType, orderRowType);
        RowType flattenType = flatten.rowType();
        RowBase[] expected = new RowBase[]{row(flattenType, 1L, "xyz", 11L, 1L, "ori"),
                                   row(itemRowType, 111L, 11L),
                                   row(itemRowType, 112L, 11L),
                                   row(flattenType, 1L, "xyz", 12L, 1L, "david"),
                                   row(itemRowType, 121L, 12L),
                                   row(itemRowType, 122L, 12L),
                                   row(flattenType, 2L, "abc", 21L, 2L, "tom"),
                                   row(itemRowType, 211L, 21L),
                                   row(itemRowType, 212L, 21L),
                                   row(flattenType, 2L, "abc", 22L, 2L, "jack"),
                                   row(itemRowType, 221L, 22L),
                                   row(itemRowType, 222L, 22L)};
        compareRows(expected, cursor(flatten, adapter));
    }

    @Test
    public void testTwoFlattens()
    {
        PhysicalOperator groupScan = groupScan_Default(coi);
        PhysicalOperator flattenCO = flatten_HKeyOrdered(groupScan, customerRowType, orderRowType);
        PhysicalOperator flattenCOI = flatten_HKeyOrdered(flattenCO, flattenCO.rowType(), itemRowType);
        RowType flattenCOIType = flattenCOI.rowType();
        RowBase[] expected = new RowBase[]{row(flattenCOIType, 1L, "xyz", 11L, 1L, "ori", 111L, 11L),
                                   row(flattenCOIType, 1L, "xyz", 11L, 1L, "ori", 112L, 11L),
                                   row(flattenCOIType, 1L, "xyz", 12L, 1L, "david", 121L, 12L),
                                   row(flattenCOIType, 1L, "xyz", 12L, 1L, "david", 122L, 12L),
                                   row(flattenCOIType, 2L, "abc", 21L, 2L, "tom", 211L, 21L),
                                   row(flattenCOIType, 2L, "abc", 21L, 2L, "tom", 212L, 21L),
                                   row(flattenCOIType, 2L, "abc", 22L, 2L, "jack", 221L, 22L),
                                   row(flattenCOIType, 2L, "abc", 22L, 2L, "jack", 222L, 22L)};
        compareRows(expected, cursor(flattenCOI, adapter));
    }

    @Test
    public void testIndexScan1()
    {
        System.out.println("customer hkeys, by customer.name");
        PhysicalOperator indexScan = indexScan_Default(index(customer, "name"));
        // TODO: Can't compare rows, because we can't yet obtain fields from index rows. So compare hkeys instead
        String[] expected = new String[]{"{1,(long)2}",
                                         "{1,(long)1}"};
        compareRenderedHKeys(expected, cursor(indexScan, adapter));
    }

    @Test
    public void testIndexScan2()
    {
        System.out.println("order hkeys, by order.salesman");
        PhysicalOperator indexScan = indexScan_Default(index(order, "salesman"));
        // TODO: Can't compare rows, because we can't yet obtain fields from index rows. So compare hkeys instead
        String[] expected = new String[]{"{1,(long)1,2,(long)12}",
                                         "{1,(long)2,2,(long)22}",
                                         "{1,(long)1,2,(long)11}",
                                         "{1,(long)2,2,(long)21}"};
        compareRenderedHKeys(expected, cursor(indexScan, adapter));
    }

    @Test
    public void testIndexLookup()
    {
        PhysicalOperator indexScan = indexScan_Default(index(order, "salesman"));
        PhysicalOperator indexLookup = indexLookup_Default(indexScan,
                                                           coi);
        RowBase[] expected = new RowBase[]{row(orderRowType, 12L, 1L, "david"),
                                   row(itemRowType, 121L, 12L),
                                   row(itemRowType, 122L, 12L),
                                   row(orderRowType, 22L, 2L, "jack"),
                                   row(itemRowType, 221L, 22L),
                                   row(itemRowType, 222L, 22L),
                                   row(orderRowType, 11L, 1L, "ori"),
                                   row(itemRowType, 111L, 11L),
                                   row(itemRowType, 112L, 11L),
                                   row(orderRowType, 21L, 2L, "tom"),
                                   row(itemRowType, 211L, 21L),
                                   row(itemRowType, 212L, 21L)};
        compareRows(expected, cursor(indexLookup, adapter));
    }

    @Test
    public void testIndexLookupWithOneAncestor()
    {
        PhysicalOperator indexScan = indexScan_Default(index(order, "salesman"));
        PhysicalOperator indexLookup = indexLookup_Default(indexScan, coi);
        PhysicalOperator exhume = ancestorLookup_Default(indexLookup, coi, orderRowType, Arrays.asList(customerRowType));
        RowBase[] expected = new RowBase[]{row(customerRowType, 1L, "xyz"),
                                   row(orderRowType, 12L, 1L, "david"),
                                   row(itemRowType, 121L, 12L),
                                   row(itemRowType, 122L, 12L),
                                   row(customerRowType, 2L, "abc"),
                                   row(orderRowType, 22L, 2L, "jack"),
                                   row(itemRowType, 221L, 22L),
                                   row(itemRowType, 222L, 22L),
                                   row(customerRowType, 1L, "xyz"),
                                   row(orderRowType, 11L, 1L, "ori"),
                                   row(itemRowType, 111L, 11L),
                                   row(itemRowType, 112L, 11L),
                                   row(customerRowType, 2L, "abc"),
                                   row(orderRowType, 21L, 2L, "tom"),
                                   row(itemRowType, 211L, 21L),
                                   row(itemRowType, 212L, 21L)};
        compareRows(expected, cursor(exhume, adapter));
    }

    @Test
    public void testIndexLookupWithTwoAncestors()
    {
        PhysicalOperator indexScan = indexScan_Default(index(item, "oid"));
        PhysicalOperator indexLookup = indexLookup_Default(indexScan, coi);
        PhysicalOperator exhume = ancestorLookup_Default(indexLookup,
                                                         coi,
                                                         itemRowType,
                                                         Arrays.asList(customerRowType, orderRowType));
        RowBase[] expected = new RowBase[]{row(customerRowType, 1L, "xyz"),
                                   row(orderRowType, 11L, 1L, "ori"),
                                   row(itemRowType, 111L, 11L),
                                   row(customerRowType, 1L, "xyz"),
                                   row(orderRowType, 11L, 1L, "ori"),
                                   row(itemRowType, 112L, 11L),
                                   row(customerRowType, 1L, "xyz"),
                                   row(orderRowType, 12L, 1L, "david"),
                                   row(itemRowType, 121L, 12L),
                                   row(customerRowType, 1L, "xyz"),
                                   row(orderRowType, 12L, 1L, "david"),
                                   row(itemRowType, 122L, 12L),
                                   row(customerRowType, 2L, "abc"),
                                   row(orderRowType, 21L, 2L, "tom"),
                                   row(itemRowType, 211L, 21L),
                                   row(customerRowType, 2L, "abc"),
                                   row(orderRowType, 21L, 2L, "tom"),
                                   row(itemRowType, 212L, 21L),
                                   row(customerRowType, 2L, "abc"),
                                   row(orderRowType, 22L, 2L, "jack"),
                                   row(itemRowType, 221L, 22L),
                                   row(customerRowType, 2L, "abc"),
                                   row(orderRowType, 22L, 2L, "jack"),
                                   row(itemRowType, 222L, 22L)};
        compareRows(expected, cursor(exhume, adapter));
    }

    @Test
    public void testRestrictedIndexScan()
    {
        Index idxOrderSalesman = index(order, "salesman");
        IndexBound lo = indexBound(userTable(order), row(order, 2, "jack"), columnSelector(idxOrderSalesman));
        IndexBound hi = indexBound(userTable(order), row(order, 2, "tom"), columnSelector(idxOrderSalesman));
        IndexKeyRange range = indexKeyRange(lo, true, hi, false);
        PhysicalOperator indexScan = indexScan_Default(idxOrderSalesman, false, range);
        // TODO: Can't compare rows, because we can't yet obtain fields from index rows. So compare hkeys instead
        String[] expected = new String[]{"{1,(long)2,2,(long)22}",
                                         "{1,(long)1,2,(long)11}"};
        compareRenderedHKeys(expected, cursor(indexScan, adapter));
    }

    @Test
    public void testRestrictedIndexLookup()
    {
        Index idxOrderSalesman = index(order, "salesman");
        IndexBound tom = indexBound(userTable(order), row(order, 2, "tom"), columnSelector(idxOrderSalesman));
        IndexKeyRange matchTom = indexKeyRange(tom, true, tom, true);
        PhysicalOperator indexScan = indexScan_Default(idxOrderSalesman, false, matchTom);
        PhysicalOperator indexLookup = indexLookup_Default(indexScan, coi);
        RowBase[] expected = new RowBase[]{row(orderRowType, 21L, 2L, "tom"),
                                   row(itemRowType, 211L, 21L),
                                   row(itemRowType, 212L, 21L)};
        compareRows(expected, cursor(indexLookup, adapter));

    }

/*
    @Test
    public void testOperatorBasedRowCollector_SecondaryIndex() throws Exception
    {
        HapiPredicate customerABC = new SimpleHapiPredicate(userTable(customer).getName(),
                                                            "name",
                                                            HapiPredicate.Operator.EQ,
                                                            "abc");
        RowCollector rc = OperatorBasedRowCollector.newCollector(session(),
                                                                 (PersistitStore) store(),
                                                                 userTable(customer),
                                                                 Arrays.asList(customerABC));
        while (rc.hasMore()) {
            RowData rowData = rc.collectNextRow();
            System.out.println(rowData);
        }
    }

    @Test
    public void testOperatorBasedRowCollector_PKIndex() throws Exception
    {
        HapiPredicate customer2 = new SimpleHapiPredicate(userTable(customer).getName(),
                                                          "cid",
                                                          HapiPredicate.Operator.EQ,
                                                          "2");
        RowCollector rc = OperatorBasedRowCollector.newCollector(session(),
                                                                 (PersistitStore) store(),
                                                                 userTable(customer),
                                                                 Arrays.asList(customer2));
        while (rc.hasMore()) {
            RowData rowData = rc.collectNextRow();
            System.out.println(rowData);
        }
        fail(); // This isn't returning any rows! Should be the same as for customer.name="abc" (previous test)
    }
*/

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

    private Index index(int userTableId, String... searchIndexColumnNamesArray)
    {
        UserTable userTable = userTable(userTableId);
        for (Index index : userTable.getIndexesIncludingInternal()) {
            List<String> indexColumnNames = new ArrayList<String>();
            for (IndexColumn indexColumn : index.getColumns()) {
                indexColumnNames.add(indexColumn.getColumn().getName());
            }
            List<String> searchIndexColumnNames = Arrays.asList(searchIndexColumnNamesArray);
            if (searchIndexColumnNames.equals(indexColumnNames)) {
                return index;
            }
        }
        return null;
    }

    private ColumnSelector columnSelector(final Index index)
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

    private RowBase row(RowType rowType, Object... fields)
    {
        return new TestRow(rowType, fields);
    }

    private RowBase row(int tableId, Object... values /* alternating field position and value */)
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

    private void compareRows(RowBase[] expected, Cursor cursor)
    {
        int count;
        try {
            cursor.open(UNDEF_BINDINGS);
            count = 0;
            List<RowBase> actualRows = new ArrayList<RowBase>(); // So that result is viewable in debugger
            while (cursor.next()) {
                RowBase actualRow = cursor.currentRow();
                if(!equal(expected[count], actualRow)) {
                    String expectedString = expected[count] == null ? "null" : expected[count].toString();
                    String actualString = actualRow == null ? "null" : actualRow.toString();
                    assertEquals("row", expectedString, actualString);
                    // just in case the strings are equal...
                    fail(String.format("%s != %s", expectedString, actualString));
                }
                count++;
                actualRows.add(actualRow);
            }
        } finally {
            cursor.close();
        }
        assertEquals(expected.length, count);
    }

    private void compareRenderedHKeys(String[] expected, Cursor cursor)
    {
        int count;
        try {
            cursor.open(UNDEF_BINDINGS);
            count = 0;
            List<RowBase> actualRows = new ArrayList<RowBase>(); // So that result is viewable in debugger
            while (cursor.next()) {
                RowBase actualRow = cursor.currentRow();
                assertEquals(expected[count], actualRow.hKey().toString());
                count++;
                actualRows.add(actualRow);
            }
        } finally {
            cursor.close();
        }
        assertEquals(expected.length, count);
    }

    private boolean equal(RowBase expected, RowBase actual)
    {
        boolean equal = expected.rowType().nFields() == actual.rowType().nFields();
        for (int i = 0; equal && i < actual.rowType().nFields(); i++) {
            Object expectedField = expected.field(i, UNDEF_BINDINGS);
            Object actualField = actual.field(i, UNDEF_BINDINGS);
            equal = expectedField.equals(actualField);
        }
        return equal;
    }

    private int customer;
    private int order;
    private int item;
    private RowType customerRowType;
    private RowType orderRowType;
    private RowType itemRowType;
    private IndexRowType customerNameIndexRowType;
    private IndexRowType orderSalesmanIndexRowType;
    private GroupTable coi;
    private Schema schema;
    private NewRow[] db;
    PersistitAdapter adapter;
}
