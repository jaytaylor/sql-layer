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

import com.akiban.ais.model.GroupIndex;
import com.akiban.qp.persistitadapter.TestOperatorStore;
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

import static org.junit.Assert.assertEquals;

public final class OperatorStoreGroupIndexIT extends ITBase {

    @Test
    public void basic() {
        writeRows(
                createNewRow(c, 1L, "alpha"),
                createNewRow(o, 10L, 1L, "01-01-2001"),
                createNewRow(o, 11L, 1L, "02-02-2002"),
                createNewRow(i, 100L, 11L, 1111),
                createNewRow(i, 101L, 11L, 2222),
                createNewRow(i, 102L, 11L, 3333),
                createNewRow(o, 12L, 1L, "03-03-2003"),
                createNewRow(a, 20L, 1L, "Harrington"),
                createNewRow(a, 21L, 1L, "Causeway"),
                createNewRow(c, 2L, "beta")
        );
        testMaintainedRows(
                createNewRow(c, 1L, "alpha"),
                // sku_name
                "[1111, alpha] hkey[1, 11, 100]",
                "[2222, alpha] hkey[1, 12, 101]",
                "[3333, alpha] hkey[1, 11, 102]",
                // street_aid_acid_cid
                "[Causeway, 20, 1] hkey[]"
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
                "date varchar(64)",
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
        final String groupName = getUserTable(SCHEMA,"customers").getGroup().getName();
        createGroupIndex(groupName, "sku_date", "items.sku, orders.date");
        createGroupIndex(groupName, "sku_name", "items.sku, customers.name");
        createGroupIndex(
                groupName,
                "street_aid_a-cid", "addresses.street, addresses.aid, addresses.c_id"
        );
    }

    @After
    public void after() {
        c = null;
        o = null;
        i = null;
        a = null;
        testOperatorStore = null;
    }

    // ApiTestBase interface

    @Override
    protected TestServiceServiceFactory createServiceFactory(Collection<Property> startupConfigProperties) {
        return new OSServiceFactory(startupConfigProperties);
    }

    // private methods

    void testMaintainedRows(NewRow targetRow, String... expectedActions) {
        RowData rowData = targetRow.toRowData();
        StringsGIHandler handler = new StringsGIHandler();
        try {
            opStore().testMaintainGroupIndexes(session(), rowData, handler);
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

    // object state

    private Integer c;
    private Integer o;
    private Integer i;
    private Integer a;
    private TestOperatorStore testOperatorStore;

    // const

    private final static String SCHEMA = "sch";

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

    private static class StringsGIHandler implements TestOperatorStore.GroupIndexHandler {

        // GroupIndexHandler interface

        @Override
        public void handleRow(GroupIndex groupIndex, List<?> fields, List<?> hKey) {
            strings.add(String.valueOf(fields) + " hkey" + String.valueOf(hKey));
        }

        // StringsGIHandler interface

        public List<String> strings() {
            return strings;
        }

        // object state

        private final List<String> strings = new ArrayList<String>();
    }
}
