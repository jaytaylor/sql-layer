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

import com.akiban.ais.model.Column;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.IndexRowComposition;
import com.akiban.qp.persistitadapter.TestOperatorStore;
import com.akiban.qp.physicaloperator.UndefBindings;
import com.akiban.qp.row.Row;
import com.akiban.server.RowData;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.service.Service;
import com.akiban.server.service.config.Property;
import com.akiban.server.store.Store;
import com.akiban.server.test.it.ITBase;
import com.akiban.util.Strings;
import com.persistit.exception.PersistitException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.akiban.qp.persistitadapter.TestOperatorStore.Action.*;
import static org.junit.Assert.assertEquals;

/**
 * <p>A test of the group index maintenance scaffolding. The scaffolding needs to send different rows to the
 * maintenance handler in different situations, summarized by the following table. We won't be testing each
 * situation, because some of them are very similar. The table will note which situation is being tested.</p>
 *
 * <table border=1>
 *  <tr>
 *      <th colspan=9>Index on (order.date, item.sku)</th>
 *  </tr>
 *  <tr>
 *      <th rowspan=2>Incoming row</th>
 *      <th colspan=8>Existing rows</th>
 *  </tr>
 *  <tr>
 *      <th>∅</th>    <th>C</th>   <th>CO</th>   <th>CI</th>
 *      <th>COI</th>  <th>O</th>   <th>OI</th>   <th>I</th>
 *  </tr>
 *  <tr>
 *      <th>C</th>
 *      <td>tested</td> <td>&nbsp;</td> <td>&nbsp;</td> <td>&nbsp;</td>
 *      <td>&nbsp;</td> <td>&nbsp;</td> <td>tested</td> <td>&nbsp;</td>
 *  </tr>
 *  <tr>
 *      <th>O</th>
 *      <td>tested</td> <td>&nbsp;</td> <td>&nbsp;</td> <td>tested</td>
 *      <td>&nbsp;</td> <td>tested</td> <td>&nbsp;</td> <td>tested</td>
 *  </tr>
 *  <tr>
 *      <th>I</th>
 *      <td>tested</td> <td>tested</td> <td>tested</td> <td>tested</td>
 *      <td>tested</td> <td>&nbsp;</td> <td>tested</td> <td>tested</td>
 *  </tr>
 * </table>
 *
 * <p>The above tests will all have the same format:
 * <ul>
 *     <li>name: {@code basic_existingAAA_incomingBBB}</li>
 *     <li>create the group index</li>
 *     <li>write the rows, including the incoming row (which will be assigned to a local var)</li>
 *     <li>test what happens when the incoming row is stored and then deleted</li>
 * </ul>
 * </p>
 *
 * <p>This works because group index maintenance is handled after normal (not-including-GIs) maintenance for storage,
 * and after it for deletion. Similarly, updates are handled by deleting the old GI entry, performing the normal update,
 * then storing the new GI entry.</p>
 *
 * <p>In addition to the above "unit-style" tests, we'll also perform some smoke tests of more complex functionality,
 * like maintenance of multiple indexes.</p>
 */
public final class OperatorStoreGroupIndexIT extends ITBase {

    // Incoming C

    @Test
    public void basic_existingNone_incomingC() {
        createGroupIndex(groupName, "date_sku", "orders.odate, items.sku");

        final NewRow target;
        writeRows(
                target = createNewRow(c, 1L, "name")
        );
        testMaintainedRows(
                STORE,
                true,
                target
        );
        testMaintainedRows(
                DELETE,
                true,
                target
        );
    }

    @Test
    public void basic_existingOI_incomingC() {
        createGroupIndex(groupName, "date_sku", "orders.odate, items.sku");

        final NewRow target;
        writeRows(
                createNewRow(o, 10L, 1L, "04-04-2004"),
                createNewRow(i, 100L, 10L, 4444),
                target = createNewRow(c, 1L, "Alpha")
        );

        testMaintainedRows(
                STORE,
                true,
                target,
                see(STORE, true, "date_sku", "[04-04-2004, 4444, 1, 10, 100]", DATE_SKU_COLS)
        );
        testMaintainedRows(
                DELETE,
                true,
                target,
                see(DELETE, true, "date_sku", "[04-04-2004, 4444, 1, 10, 100]", DATE_SKU_COLS)
        );
    }

    // Incoming O

    @Test
    public void basic_existingNone_incomingO() {
        createGroupIndex(groupName, "date_sku", "orders.odate, items.sku");

        final NewRow target;
        writeRows(
                target = createNewRow(o, 10L, 1L, "01-01-2001")
        );
        testMaintainedRows(
                STORE,
                true,
                target
        );
        testMaintainedRows(
                DELETE,
                true,
                target
        );
    }

    @Test
    public void basic_existingCI_incomingO() {
        createGroupIndex(groupName, "date_sku", "orders.odate, items.sku");

        final NewRow target;
        writeRows(
                createNewRow(c, 1L, "alpha"),
                createNewRow(i, 100, 10L, 1111),
                target = createNewRow(o, 10L, 1L, "01-01-2001")
        );

        testMaintainedRows(
                STORE,
                true,
                target,
                see(STORE, true, "date_sku", "[01-01-2001, 1111, 1, 10, 100]", DATE_SKU_COLS)
        );
        testMaintainedRows(
                DELETE,
                true,
                target,
                see(DELETE, true, "date_sku", "[01-01-2001, 1111, 1, 10, 100]", DATE_SKU_COLS)
        );
    }

    @Test
    public void basic_existingO_incomingO() {
        createGroupIndex(groupName, "date_sku", "orders.odate, items.sku");

        final NewRow target;
        writeRows(
                createNewRow(o, 10L, 1L, "01-01-2001"),
                target = createNewRow(o, 11L, 1L, "02-02-2002")
        );

        testMaintainedRows(
                STORE,
                true,
                target
        );
        testMaintainedRows(
                DELETE,
                true,
                target
        );
    }

    @Test
    public void basic_existingI_incomingO() {
        createGroupIndex(groupName, "date_sku", "orders.odate, items.sku");

        final NewRow target;
        writeRows(
                createNewRow(i, 100L, 10L, 1111),
                target = createNewRow(o, 10L, 1L, "03-03-2003")
        );

        testMaintainedRows(
                STORE,
                true,
                target,
                see(STORE, true, "date_sku", "[03-03-2003, 1111, 1, 10, 100]", DATE_SKU_COLS)
        );
        testMaintainedRows(
                DELETE,
                true,
                target,
                see(DELETE, true, "date_sku", "[03-03-2003, 1111, 1, 10, 100]", DATE_SKU_COLS)
        );
    }

    // Incoming I

    @Test
    public void basic_existingNone_incomingI() {
        createGroupIndex(groupName, "date_sku", "orders.odate, items.sku");

        final NewRow target;
        writeRows(
                target = createNewRow(i, 100L, 10L, 1111)
        );
        testMaintainedRows(
                STORE,
                true,
                target
        );
        testMaintainedRows(
                DELETE,
                true,
                target
        );
    }

    @Test
    public void basic_existingC_incomingI() {
        createGroupIndex(groupName, "date_sku", "orders.odate, items.sku");

        final NewRow target;
        writeRows(
                createNewRow(c, 1L, "one"),
                target = createNewRow(i, 100L, 10L, 1111)
        );

        testMaintainedRows(
                STORE,
                true,
                target
        );
        testMaintainedRows(
                DELETE,
                true,
                target
        );
    }

    @Test
    public void basic_existingCO_incomingI() {
        createGroupIndex(groupName, "date_sku", "orders.odate, items.sku");

        final NewRow target;
        writeRows(
                createNewRow(c, 1L, "one"),
                createNewRow(o, 10L, 1L, "1-1-01"),
                target = createNewRow(i, 100L, 10L, 11111)
        );

        testMaintainedRows(
                STORE,
                true,
                target,
                see(STORE, true, "date_sku", "[1-1-01, 11111, 1, 10, 100]", DATE_SKU_COLS)
        );
        testMaintainedRows(
                DELETE,
                true,
                target,
                see(DELETE, true, "date_sku", "[1-1-01, 11111, 1, 10, 100]", DATE_SKU_COLS)
        );
    }

    @Test
    public void basic_existingCI_incomingI() {
        createGroupIndex(groupName, "date_sku", "orders.odate, items.sku");

        final NewRow target;
        writeRows(
                createNewRow(c, 1L, "alpha"),
                createNewRow(i, 100L, 10L, 1111),
                target = createNewRow(i, 101L, 10L, 2222)
        );

        testMaintainedRows(
                STORE,
                true,
                target
        );
        testMaintainedRows(
                DELETE,
                true,
                target
        );
    }

    @Test
    public void basic_existingCOI_incomingI() {
        createGroupIndex(groupName, "date_sku", "orders.odate, items.sku");

        final NewRow target;
        writeRows(
                createNewRow(c, 1L, "customer one"),
                createNewRow(o, 10L, 1L, "01-01-2001"),
                createNewRow(i, 100L, 10L, 1111),
                target = createNewRow(i, 101L, 10L, 2222)
        );

        testMaintainedRows(
                STORE,
                true,
                target,
                see(STORE, true, "date_sku", "[01-01-2001, 2222, 1, 10, 101]", DATE_SKU_COLS)
        );
        testMaintainedRows(
                DELETE,
                true,
                target,
                see(DELETE, true, "date_sku", "[01-01-2001, 2222, 1, 10, 101]", DATE_SKU_COLS)
        );
    }

    @Test
    public void basic_existingOI_incomingI() {
        createGroupIndex(groupName, "date_sku", "orders.odate, items.sku");

        final NewRow target;
        writeRows(
                createNewRow(o, 10L, 1L, "2001-01-01"),
                createNewRow(i, 100L, 10L, 1234),
                target = createNewRow(i, 101L, 10L, 5678)
        );

        testMaintainedRows(
                STORE,
                true,
                target,
                see(STORE, true, "date_sku", "[2001-01-01, 5678, 1, 10, 101]", DATE_SKU_COLS)
        );
        testMaintainedRows(
                DELETE,
                true,
                target,
                see(DELETE, true, "date_sku", "[2001-01-01, 5678, 1, 10, 101]", DATE_SKU_COLS)
        );
    }

    @Test
    public void basic_existingI_incomingI() {
        createGroupIndex(groupName, "date_sku", "orders.odate, items.sku");

        final NewRow target;
        writeRows(
                createNewRow(i, 100L, 10L, 1234),
                target = createNewRow(i, 101L, 10L, 5678)
        );

        testMaintainedRows(
                STORE,
                true,
                target
        );
        testMaintainedRows(
                DELETE,
                true,
                target
        );
    }

    // Before and After

    @Before
    public void createTables() {
        testOperatorStore = (TestOperatorStore) store();
        c = createTable(SCHEMA, "customers",
                "cid int key",
                "name varchar(32)"
        );
        o = createTable(SCHEMA, "orders",
                "oid int key",
                "c_id int",
                "odate varchar(64)",
                "CONSTRAINT __akiban_o FOREIGN KEY __akiban_o(c_id) REFERENCES customers(cid)"
        );
        i = createTable(SCHEMA, "items",
                "iid int key",
                "o_id int",
                "sku int",
                "CONSTRAINT __akiban_i FOREIGN KEY __akiban_i(o_id) REFERENCES orders(oid)"
        );
        a = createTable(SCHEMA, "addresses",
                "aid int key",
                "c_id int",
                "street varchar(64)",
                "CONSTRAINT __akiban_o FOREIGN KEY __akiban_o(c_id) REFERENCES customers(cid)"
        );

        groupName = getUserTable(SCHEMA,"customers").getGroup().getName();
    }

    @After
    public void after() {
        c = null;
        o = null;
        i = null;
        a = null;
        groupName = null;
        testOperatorStore = null;
    }

    // ApiTestBase interface

    @Override
    protected TestServiceServiceFactory createServiceFactory(Collection<Property> startupConfigProperties) {
        return new OSServiceFactory(startupConfigProperties);
    }

    // private methods

    private void testMaintainedRows(TestOperatorStore.Action action, boolean alsoNullKeys, NewRow targetRow, String... expectedActions) {
        RowData rowData = targetRow.toRowData();
        StringsGIHandler handler = new StringsGIHandler();
        try {
            opStore().testMaintainGroupIndexes(session(), rowData, handler, action, alsoNullKeys);
        } catch (PersistitException e) {
            throw new RuntimeException(e);
        }
        List<String> actual = Arrays.asList(expectedActions);
        List<String> expected = handler.strings();
        if (!expected.equals(actual)) {
            assertEquals("updates for " + targetRow, Strings.join(actual), Strings.join(expected));
            // and just in case...
            assertEquals("updates for " + targetRow, actual, expected);
        }
    }

    private TestOperatorStore opStore() {
        return testOperatorStore;
    }

    private static String see(Enum<?> action, boolean alsoNullFields, String indexName, String fields, String columns) {
        return String.format("%s (%s) on %s: fields=%s cols=%s", action, alsoNullFields, indexName, fields, columns);
    }

    // object state

    private Integer c;
    private Integer o;
    private Integer i;
    private Integer a;
    private String groupName;
    private TestOperatorStore testOperatorStore;

    // const

    private final static String SCHEMA = "sch";
    private static final String DATE_SKU_COLS = "[orders.odate, items.sku, orders.c_id, orders.oid, items.iid]";
    private static final String SKU_NAME_COLS = "[items.sku, customers.name, customers.cid, orders.oid, items.iid]";
    private static final String STREET_AID_CID_COLS = "[addresses.street, addresses.aid, customers.cid]";

    // nested classes

    private static class OSServiceFactory extends TestServiceServiceFactory {
        private OSServiceFactory(Collection<Property> startupConfigProperties) {
            super(startupConfigProperties);
        }

        @Override
        public Service<Store> storeService() {
            return new TestOperatorStore();
        }
    }

    private static class StringsGIHandler implements TestOperatorStore.GroupIndexHandler<RuntimeException> {

        // GroupIndexHandler interface

        @Override
        public void handleRow(GroupIndex groupIndex, Row row, Action action, boolean alsoNullKeys) {
            List<Object> fields = new ArrayList<Object>();
            List<Column> columns = new ArrayList<Column>();
            IndexRowComposition irc = groupIndex.indexRowComposition();
            for (int i=0; i < irc.getLength(); ++i ) {
                assert irc.isInRowData(i);
                assert ! irc.isInHKey(i);
                final int rowIndex = irc.getFieldPosition(i);
                fields.add(row.field(rowIndex, UndefBindings.only()));
                columns.add(groupIndex.getColumnForFlattenedRow(rowIndex));
            }
            String s = see(
                    action,
                    alsoNullKeys,
                    groupIndex.getIndexName().getName(),
                    fields.toString(),
                    columns.toString()
            );
            strings.add(s);
        }

        // StringsGIHandler interface

        public List<String> strings() {
            return strings;
        }

        // object state

        private final List<String> strings = new ArrayList<String>();
    }
}
