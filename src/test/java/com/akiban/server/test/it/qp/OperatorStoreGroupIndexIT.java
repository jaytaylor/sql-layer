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

import com.akiban.qp.persistitadapter.TestOperatorStore;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.service.Service;
import com.akiban.server.service.config.Property;
import com.akiban.server.store.Store;
import com.akiban.server.test.it.ITBase;
import com.akiban.util.Strings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

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

        final NewRow target = createNewRow(c, 1L, "name");

        testMaintainedRows(
                Action.STORE,
                target
        );
        testMaintainedRows(
                Action.DELETE,
                target
        );
    }

    @Test
    public void basic_existingOI_incomingC() {
        createGroupIndex(groupName, "date_sku", "orders.odate, items.sku");

        writeRows(
                createNewRow(o, 10L, 1L, "04-04-2004"),
                createNewRow(i, 100L, 10L, 4444)
        );
        final NewRow target = createNewRow(c, 1L, "Alpha");

        testMaintainedRows(
                Action.STORE,
                target
        );
        testMaintainedRows(
                Action.DELETE,
                target
        );
    }

    // Incoming O

    @Test
    public void basic_existingNone_incomingO() {
        createGroupIndex(groupName, "date_sku", "orders.odate, items.sku");

        final NewRow target = createNewRow(o, 10L, 1L, "01-01-2001");

        testMaintainedRows(
                Action.STORE,
                target,
                seeStore("date_sku", depth(o), "01-01-2001", null, 1L, 10L, null)
        );
        testMaintainedRows(
                Action.DELETE,
                target,
                seeRemove("date_sku", "01-01-2001", null, 1L, 10L, null)
        );
    }

    @Test
    public void basic_existingCI_incomingO() {
        createGroupIndex(groupName, "date_sku", "orders.odate, items.sku");

        writeRows(
                createNewRow(c, 1L, "alpha"),
                createNewRow(i, 100, 10L, 1111)
        );
        final NewRow target = createNewRow(o, 10L, 1L, "01-01-2001");

                testMaintainedRows(
                Action.STORE,
                target,
                seeStore("date_sku", depth(i), "01-01-2001", 1111L, 1L, 10L, 100L)
        );
        testMaintainedRows(
                Action.DELETE,
                target,
                seeRemove("date_sku", "01-01-2001", 1111L, 1L, 10L, 100L)
        );
    }

    @Test
    public void basic_existingO_incomingO() {
        createGroupIndex(groupName, "date_sku", "orders.odate, items.sku");

        writeRows(
                createNewRow(o, 10L, 1L, "01-01-2001")
        );
        final NewRow target = createNewRow(o, 11L, 1L, "02-02-2002");

        testMaintainedRows(
                Action.STORE,
                target,
                seeStore("date_sku", depth(o), "02-02-2002", null, 1L, 11L, null)
        );
        testMaintainedRows(
                Action.DELETE,
                target,
                seeRemove("date_sku", "02-02-2002", null, 1L, 11L, null)
        );
    }

    @Test
    public void basic_existingI_incomingO() {
        createGroupIndex(groupName, "date_sku", "orders.odate, items.sku");

        writeRows(
                createNewRow(i, 100L, 10L, 1111)
        );
        final NewRow target = createNewRow(o, 10L, 1L, "03-03-2003");

        testMaintainedRows(
                Action.STORE,
                target,
                seeStore("date_sku", depth(i), "03-03-2003", 1111L, 1L, 10L, 100L)
        );
        testMaintainedRows(
                Action.DELETE,
                target,
                seeRemove("date_sku", "03-03-2003", 1111L, 1L, 10L, 100L)
        );
    }

    // Incoming I

    @Test
    public void basic_existingNone_incomingI() {
        createGroupIndex(groupName, "date_sku", "orders.odate, items.sku");

        final NewRow target = createNewRow(i, 100L, 10L, 1111);
        testMaintainedRows(
                Action.STORE,
                target
        );
        testMaintainedRows(
                Action.DELETE,
                target
        );
    }

    @Test
    public void basic_existingC_incomingI() {
        createGroupIndex(groupName, "date_sku", "orders.odate, items.sku");

        writeRows(
                createNewRow(c, 1L, "one")
        );
        final NewRow target = createNewRow(i, 100L, 10L, 1111);

        testMaintainedRows(
                Action.STORE,
                target
        );
        testMaintainedRows(
                Action.DELETE,
                target
        );
    }

    @Test
    public void basic_existingCO_incomingI() {
        createGroupIndex(groupName, "date_sku", "orders.odate, items.sku");

        writeRows(
                createNewRow(c, 1L, "one"),
                createNewRow(o, 10L, 1L, "1-1-01")
        );
        final NewRow target = createNewRow(i, 100L, 10L, 11111);

        testMaintainedRows(
                Action.STORE,
                target,
                seeStore("date_sku", depth(i), "1-1-01", 11111L, 1L, 10L, 100L),
                seeRemove("date_sku", "1-1-01", null, 1L, 10L, null)
        );
        testMaintainedRows(
                Action.DELETE,
                target,
                seeRemove("date_sku", "1-1-01", 11111L, 1L, 10L, 100L),
                seeStore("date_sku", depth(o), "1-1-01", null, 1L, 10L, null)
        );
    }

    @Test
    public void basic_existingCI_incomingI() {
        createGroupIndex(groupName, "date_sku", "orders.odate, items.sku");

        writeRows(
                createNewRow(c, 1L, "alpha"),
                createNewRow(i, 100L, 10L, 1111)
        );
        final NewRow target = createNewRow(i, 101L, 10L, 2222);

        testMaintainedRows(
                Action.STORE,
                target
        );
        testMaintainedRows(
                Action.DELETE,
                target
        );
    }

    @Test
    public void basic_existingCOI_incomingI() {
        createGroupIndex(groupName, "date_sku", "orders.odate, items.sku");

        writeRows(
                createNewRow(c, 1L, "customer one"),
                createNewRow(o, 10L, 1L, "01-01-2001"),
                createNewRow(i, 100L, 10L, 1111)
        );
        final NewRow target = createNewRow(i, 101L, 10L, 2222);

        testMaintainedRows(
                Action.STORE,
                target,
                seeRemove("date_sku", "01-01-2001", null, 1L, 10L, null),
                seeStore("date_sku", depth(i), "01-01-2001", 2222L, 1L, 10L, 101L)
        );
        testMaintainedRows(
                Action.DELETE,
                target,
                seeRemove("date_sku", "01-01-2001", 2222L, 1L, 10L, 101L)
        );
    }

    @Test
    public void basic_existingOI_incomingI() {
        createGroupIndex(groupName, "date_sku", "orders.odate, items.sku");

        writeRows(
                createNewRow(o, 10L, 1L, "2001-01-01"),
                createNewRow(i, 100L, 10L, 1234)
        );
        final NewRow target = createNewRow(i, 101L, 10L, 5678);

        testMaintainedRows(
                Action.STORE,
                target,
                seeRemove("date_sku", "2001-01-01", null, 1L, 10L, null),
                seeStore("date_sku", depth(i), "2001-01-01", 5678L, 1L, 10L, 101L)
        );
        testMaintainedRows(
                Action.DELETE,
                target,
                seeRemove("date_sku", "2001-01-01", 5678L, 1L, 10L, 101L)
        );
    }

    @Test
    public void basic_existingI_incomingI() {
        createGroupIndex(groupName, "date_sku", "orders.odate, items.sku");

        writeRows(
                createNewRow(i, 100L, 10L, 1234)
        );
        final NewRow target = createNewRow(i, 101L, 10L, 5678);

        testMaintainedRows(
                Action.STORE,
                target
        );
        testMaintainedRows(
                Action.DELETE,
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

    private int depth(int tableId) {
        return ddl().getAIS(session()).getUserTable(tableId).getDepth();
    }

    private void testMaintainedRows(Action action, NewRow targetRow, String... expectedActions) {
        opStore().clearHookStrings();

        switch (action) {
        case DELETE:
            dml().deleteRow(session(), targetRow);
            break;
        case STORE:
            dml().writeRow(session(), targetRow);
            break;
        default:
            throw new AssertionError(action.name());
        }

        List<String> expected = Arrays.asList(expectedActions);
        List<String> actual = opStore().getAndClearHookStrings();
        Collections.sort(expected);
        Collections.sort(actual);
        if (!expected.equals(actual)) {
            assertEquals("hook strings", Strings.join(expected), Strings.join(actual));
            // just in case...
            assertEquals("hook strings", expected, actual);
        }
    }

    private TestOperatorStore opStore() {
        return testOperatorStore;
    }

    private static String seeStore(String indexName, int depth, Object... key) {
        return String.format("STORE to %s %s => %s", indexName, keyString(key), depth);
    }

    private static String seeRemove(String indexName, Object... key) {
        return String.format("REMOVE from %s %s", indexName, keyString(key));
    }

    private static String keyString(Object... key) {
        StringBuilder builder = new StringBuilder();
        builder.append('{');
        for(int i=0; i < key.length; ++i) {
            Object elem = key[i];
            if (elem == null) {
                builder.append("null");
            }
            else if (elem instanceof Long) {
                builder.append("(long)").append(elem);
            }
            else if (elem instanceof String) {
                builder.append('"').append(elem).append('"');
            }
            else {
                throw new UnsupportedOperationException(elem + " is of class " + elem.getClass());
            }

            if (i < key.length-1) {
                builder.append(',');
            }
        }
        builder.append('}');
        return builder.toString();
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

    private enum Action {
        STORE,
        DELETE
    }
}
