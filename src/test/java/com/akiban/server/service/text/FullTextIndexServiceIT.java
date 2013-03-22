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

package com.akiban.server.service.text;

import com.akiban.ais.model.FullTextIndex;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.QueryContext;
import static com.akiban.qp.operator.API.cursor;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.util.SchemaCache;
import com.akiban.server.service.servicemanager.GuicedServiceManager;
import com.akiban.server.test.it.ITBase;
import com.akiban.server.test.it.qp.TestRow;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.*;

public class FullTextIndexServiceIT extends ITBase
{
    public static final String SCHEMA = "test";

    protected FullTextIndexService fullText;
    protected Schema schema;
    protected PersistitAdapter adapter;
    protected QueryContext queryContext;

    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
                .bindAndRequire(FullTextIndexService.class, FullTextIndexServiceImpl.class);
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("akserver.text.indexpath", "/tmp/aktext");
        return properties;
    }

    @Before
    public void createData() {
        int c = createTable(SCHEMA, "c",
                            "cid INT PRIMARY KEY NOT NULL",
                            "name VARCHAR(128) COLLATE en_us_ci");
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
        writeRow(c, 1, "Fred Flintstone");
        writeRow(o, 101, 1, "2012-12-12");
        writeRow(i, 10101, 101, "ABCD");
        writeRow(i, 10102, 101, "1234");
        writeRow(o, 102, 1, "2013-01-01");
        writeRow(a, 101, 1, "MA");
        writeRow(c, 2, "Barney Rubble");
        writeRow(a, 201, 2, "NY");
        writeRow(c, 3, "Wilma Flintstone");
        writeRow(o, 301, 3, "2010-04-01");
        writeRow(a, 301, 3, "MA");
        writeRow(a, 302, 3, "ME");

        fullText = serviceManager().getServiceByClass(FullTextIndexService.class);

        schema = SchemaCache.globalSchema(ais());
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
    }

    @Test
    public void cDown() {
        FullTextIndex index = createFullTextIndex(SCHEMA, "c", "idx_c", 
                                                  "name", "i.sku", "a.state");
        fullText.createIndex(session(), index.getIndexName());

        RowType rowType = rowType("c");
        RowBase[] expected = new RowBase[] {
            row(rowType, 1L),
            row(rowType, 3L)
        };
        FullTextQueryBuilder builder = new FullTextQueryBuilder(index, ais(), queryContext);
        Operator plan = builder.scanOperator("flintstone", 10);
        compareRows(expected, cursor(plan, queryContext));

        plan = builder.scanOperator("state:MA", 10);
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void oUpDown() {
        FullTextIndex index = createFullTextIndex(SCHEMA, "o", "idx_o",
                                                  "c.name", "i.sku");
        fullText.createIndex(session(), index.getIndexName());

        RowType rowType = rowType("o");
        RowBase[] expected = new RowBase[] {
            row(rowType, 1L, 101L)
        };
        FullTextQueryBuilder builder = new FullTextQueryBuilder(index, ais(), queryContext);
        Operator plan = builder.scanOperator("name:Flintstone AND sku:1234", 10);
        compareRows(expected, cursor(plan, queryContext));
    }

    protected RowType rowType(String tableName) {
        return schema.newHKeyRowType(ais().getUserTable(SCHEMA, tableName).hKey());
    }

    protected TestRow row(RowType rowType, Object... fields) {
        return new TestRow(rowType, fields);
    }

}
