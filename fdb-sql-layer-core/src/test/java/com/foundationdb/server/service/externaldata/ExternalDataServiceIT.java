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

package com.foundationdb.server.service.externaldata;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import com.foundationdb.server.test.it.ITBase;
import com.foundationdb.server.test.it.qp.TestRow;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.SimpleQueryContext;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.util.SchemaCache;

import com.foundationdb.server.types.FormatOptions;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ExternalDataServiceIT extends ITBase
{
    public static final String SCHEMA = "test";
    public static final boolean WITH_TXN = true;
    private static FormatOptions options;

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
        
        options = new FormatOptions();
        ConfigurationService configService = configService();
        options.set(FormatOptions.JsonBinaryFormatOption.fromProperty(configService.getProperty("fdbsql.sql.jsonbinary_output")));

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
                                  Arrays.asList(Collections.singletonList((Object)"1"),
                                                Collections.singletonList((Object)"3")),
                                  -1,
                                  WITH_TXN,
                                  options);
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
                                  Collections.singletonList(Collections.singletonList((Object)"101")),
                                  -1,
                                  WITH_TXN,
                                  options);
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
                                  Collections.singletonList(Collections.singletonList((Object)"1")),
                                  0,
                                  WITH_TXN,
                                  options);
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
                                  Collections.singletonList(Collections.singletonList((Object)"1")),
                                  1,
                                  WITH_TXN,
                                  options);
        assertEquals(C1d1, str.toString());
    }

    @Test
    public void dumpJsonEmpty() throws IOException {
        ExternalDataService external = 
            serviceManager().getServiceByClass(ExternalDataService.class);
        StringWriter str = new StringWriter();
        PrintWriter pw = new PrintWriter(str);
        external.dumpBranchAsJson(session(), pw, SCHEMA, "c", 
                                  Collections.singletonList(Collections.singletonList((Object)"666")),
                                  -1,
                                  WITH_TXN, 
                                  options);
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
                                  Collections.singletonList(Collections.singletonList((Object)"1")),
                                  -1,
                                  WITH_TXN,
                                  options);
        assertEquals(fooA1BarB1, str.toString());
    }

    static final String CSV = "Fred\nWilma\nBarney\nBetty\n";

    @Test
    public void loadCsvWithFunctions() throws Exception {
        ExternalDataService external =
                serviceManager().getServiceByClass(ExternalDataService.class);
        int tid = createTable(SCHEMA, "t",
                              "id INT PRIMARY KEY NOT NULL GENERATED BY DEFAULT AS IDENTITY",
                              "name VARCHAR(128)",
                              "source VARCHAR(128) DEFAULT CURRENT SCHEMA");
        AkibanInformationSchema ais = ais();
        Table table = ais.getTable(tid);
        List<Column> columns = Collections.singletonList(table.getColumn("name"));
        InputStream istr = new ByteArrayInputStream(CSV.getBytes("UTF-8"));
        Schema schema = SchemaCache.globalSchema(ais);
        StoreAdapter adapter = newStoreAdapter(schema);
        QueryContext queryContext = new SimpleQueryContext(adapter) {
                @Override
                public ServiceManager getServiceManager() {
                    return serviceManager();
                }
                @Override
                public String getCurrentSchema() {
                    return SCHEMA;
                }
            };
        long nrows = external.loadTableFromCsv(session(), istr, new CsvFormat("UTF-8"),
                                               0, table, columns,
                                               -1, 1, queryContext);
        assertEquals(4, nrows);
        RowType rowType = schema.tableRowType(table);
        compareRows(new Row[] {
                        testRow(rowType, 1L, "Fred", SCHEMA),
                        testRow(rowType, 2L, "Wilma", SCHEMA),
                        testRow(rowType, 3L, "Barney", SCHEMA),
                        testRow(rowType, 4L, "Betty", SCHEMA)
                    },
                    adapter.newGroupCursor(table.getGroup()));
    }
}
