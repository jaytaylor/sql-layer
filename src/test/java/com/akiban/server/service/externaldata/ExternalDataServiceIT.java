/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.server.service.externaldata;

import com.akiban.server.service.servicemanager.GuicedServiceManager;
import com.akiban.server.test.it.ITBase;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class ExternalDataServiceIT extends ITBase
{
    public static final String SCHEMA = "test";
    public static final boolean WITH_TXN = true;

    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
                .bindAndRequire(ExternalDataService.class, ExternalDataServiceImpl.class);
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        return uniqueStartupConfigProperties(getClass());
    }

    @Before
    public void createData() {
        int c = createTable(SCHEMA, "c",
                            "cid INT PRIMARY KEY NOT NULL",
                            "name VARCHAR(32)");
        int o = createTable(SCHEMA, "o",
                            "oid INT PRIMARY KEY NOT NULL",
                            "cid INT NOT NULL",
                            "GROUPING FOREIGN KEY(cid) REFERENCES c(cid)",
                            "order_date DATE");
        int i = createTable(SCHEMA, "i",
                            "iid INT PRIMARY KEY NOT NULL",
                            "oid INT NOT NULL",
                            "GROUPING FOREIGN KEY(oid) REFERENCES o(oid)",
                            "sku VARCHAR(10) NOT NULL");
        int a = createTable(SCHEMA, "a",
                            "aid INT PRIMARY KEY NOT NULL",
                            "cid INT NOT NULL",
                            "GROUPING FOREIGN KEY(cid) REFERENCES c(cid)",
                            "state CHAR(2)");
        writeRow(c, 1, "Smith");
        writeRow(o, 101, 1, "2012-12-12");
        writeRow(i, 10101, 101, "ABCD");
        writeRow(i, 10102, 101, "1234");
        writeRow(o, 102, 1, "2013-01-01");
        writeRow(a, 101, 1, "MA");
        writeRow(c, 2, "Jones");
        writeRow(a, 201, 2, "NY");
        writeRow(c, 3, "Adams");
        writeRow(o, 301, 3, "2010-04-01");
    }

    static final String C13 = "[\n" +
        "{\"cid\":1,\"name\":\"Smith\",\"o\":[{\"oid\":101,\"cid\":1,\"order_date\":\"2012-12-12\",\"i\":[{\"iid\":10101,\"oid\":101,\"sku\":\"ABCD\"},{\"iid\":10102,\"oid\":101,\"sku\":\"1234\"}]},{\"oid\":102,\"cid\":1,\"order_date\":\"2013-01-01\"}],\"a\":[{\"aid\":101,\"cid\":1,\"state\":\"MA\"}]},\n" +
        "{\"cid\":3,\"name\":\"Adams\",\"o\":[{\"oid\":301,\"cid\":3,\"order_date\":\"2010-04-01\"}]}\n" +
        "]";

    @Test
    public void dumpJsonC13() throws IOException {
        ExternalDataService external =
            serviceManager().getServiceByClass(ExternalDataService.class);
        StringWriter str = new StringWriter();
        PrintWriter pw = new PrintWriter(str);
        external.dumpBranchAsJson(session(), pw, SCHEMA, "c",
                                  Arrays.asList(Collections.singletonList("1"),
                                                Collections.singletonList("3")),
                                  -1,
                                  WITH_TXN);
        assertEquals(C13, str.toString());
    }

    static final String O101 = "[\n" +
        "{\"oid\":101,\"cid\":1,\"order_date\":\"2012-12-12\",\"i\":[{\"iid\":10101,\"oid\":101,\"sku\":\"ABCD\"},{\"iid\":10102,\"oid\":101,\"sku\":\"1234\"}]}\n" +
        "]";

    @Test
    public void dumpJsonO101() throws IOException {
        ExternalDataService external =
            serviceManager().getServiceByClass(ExternalDataService.class);
        StringWriter str = new StringWriter();
        PrintWriter pw = new PrintWriter(str);
        external.dumpBranchAsJson(session(), pw, SCHEMA, "o",
                                  Collections.singletonList(Collections.singletonList("101")),
                                  -1,
                                  WITH_TXN);
        assertEquals(O101, str.toString());
    }

    static final String C1d0 = "[\n" +
            "{\"cid\":1,\"name\":\"Smith\"}\n" +
            "]";

    @Test
    public void dumpJsonDepth0() throws IOException {
        ExternalDataService external =
                serviceManager().getServiceByClass(ExternalDataService.class);
        StringWriter str = new StringWriter();
        PrintWriter pw = new PrintWriter(str);
        external.dumpBranchAsJson(session(), pw, SCHEMA, "c",
                                  Collections.singletonList(Collections.singletonList("1")),
                                  0,
                                  WITH_TXN);
        assertEquals(C1d0, str.toString());
    }

    static final String C1d1 = "[\n" +
        "{\"cid\":1,\"name\":\"Smith\",\"o\":[{\"oid\":101,\"cid\":1,\"order_date\":\"2012-12-12\"},{\"oid\":102,\"cid\":1,\"order_date\":\"2013-01-01\"}],\"a\":[{\"aid\":101,\"cid\":1,\"state\":\"MA\"}]}\n" +
        "]";

    @Test
    public void dumpJsonDepth1() throws IOException {
        ExternalDataService external = 
            serviceManager().getServiceByClass(ExternalDataService.class);
        StringWriter str = new StringWriter();
        PrintWriter pw = new PrintWriter(str);
        external.dumpBranchAsJson(session(), pw, SCHEMA, "c", 
                                  Collections.singletonList(Collections.singletonList("1")),
                                  1,
                                  WITH_TXN);
        assertEquals(C1d1, str.toString());
    }

    @Test
    public void dumpJsonEmpty() throws IOException {
        ExternalDataService external = 
            serviceManager().getServiceByClass(ExternalDataService.class);
        StringWriter str = new StringWriter();
        PrintWriter pw = new PrintWriter(str);
        external.dumpBranchAsJson(session(), pw, SCHEMA, "c", 
                                  Collections.singletonList(Collections.singletonList("666")),
                                  -1,
                                  WITH_TXN);
        assertEquals("[]", str.toString());
    }


    static final String fooA1BarB1 = "[\n" +
            "{\"aid\":1,\"bar.b\":[{\"bid\":10,\"aid\":1}]}\n" +
            "]";

    @Test
    public void dumpJsonCrossSchemaGroup() {
        ExternalDataService external =
                serviceManager().getServiceByClass(ExternalDataService.class);
        int aid = createTable("foo", "a",
                              "aid INT PRIMARY KEY NOT NULL");
        int bid = createTable("bar", "b",
                              "bid INT PRIMARY KEY NOT NULL",
                              "aid INT NOT NULL",
                              "GROUPING FOREIGN KEY(aid) REFERENCES foo.a(aid)");
        writeRow(aid, 1L);
        writeRow(bid, 10L, 1L);
        StringWriter str = new StringWriter();
        PrintWriter pw = new PrintWriter(str);
        external.dumpBranchAsJson(session(), pw, "foo", "a",
                                  Collections.singletonList(Collections.singletonList("1")),
                                  -1,
                                  WITH_TXN);
        assertEquals(fooA1BarB1, str.toString());
    }
}
