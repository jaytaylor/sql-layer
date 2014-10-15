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

package com.foundationdb.server.service.text;

import com.foundationdb.ais.model.FullTextIndex;
import com.foundationdb.qp.operator.Operator;
import static com.foundationdb.qp.operator.API.cursor;

import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import com.foundationdb.server.service.transaction.TransactionService.CloseableTransaction;
import com.foundationdb.server.test.it.qp.TestRow;

import org.junit.Before;
import org.junit.Test;

public class FullTextIndexServiceIT extends FullTextIndexServiceITBase
{
    public static final String SCHEMA = "test";
    
    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
                .bindAndRequire(FullTextIndexService.class, FullTextIndexServiceImpl.class);
    }

    @Before
    public void createData() {
        c = createTable(SCHEMA, "c",
                        "cid INT PRIMARY KEY NOT NULL",
                        "name VARCHAR(128) COLLATE en_us_ci");
        o = createTable(SCHEMA, "o",
                        "oid INT PRIMARY KEY NOT NULL",
                        "cid INT NOT NULL",
                        "GROUPING FOREIGN KEY(cid) REFERENCES c(cid)",
                        "order_date DATE");
        i = createTable(SCHEMA, "i",
                        "iid INT PRIMARY KEY NOT NULL",
                        "oid INT NOT NULL",
                        "GROUPING FOREIGN KEY(oid) REFERENCES o(oid)",
                        "sku VARCHAR(10) NOT NULL");
        a = createTable(SCHEMA, "a",
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

        schema = SchemaCache.globalSchema(ais());
        adapter = newStoreAdapter(schema);
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
    }

    @Test
    public void testUpdate() throws InterruptedException
    {
        /*
            Test plans:
            1 - do a search, confirm that the rows come back as expected
            2 - (With update worker enabled)
                + insert new rows
                + wait for the updates to be done
                + do the search again and confirm that the new rows are found
            3 - (With update worker NOT enable)
                + disable update worker
                + insert new rows
                + do the search again and confirm that the new rows are NOT found

         */

        //CREATE INDEX cust_ft ON customers(FULL_TEXT(name, addresses.state, items.sku))

        // part 1
        FullTextIndex index = createFullTextIndex(
                                                  SCHEMA, "c", "idx_c", 
                                                  "name", "i.sku", "a.state");

        RowType rowType = rowType("c");
        Row[] expected1 = new Row[]
        {
            row(rowType, 1L),
            row(rowType, 3L)
        };
        FullTextQueryBuilder builder = new FullTextQueryBuilder(index, ais(), queryContext);
        ftScanAndCompare(builder, "flintstone", 15, expected1);
        ftScanAndCompare(builder, "state:MA", 15, expected1);

        // part 2
        // write new rows
        writeRow(c, 4, "John Watson");
        writeRow(c, 5, "Sherlock Flintstone");
        writeRow(c, 6, "Mycroft Holmes");
        writeRow(c, 7, "Flintstone Lestrade");

        waitUpdate();
        Row expected2[] = new Row[]
        {
            row(rowType, 1L),
            row(rowType, 3L),
            row(rowType, 5L),
            row(rowType, 7L)
        };

        // confirm new changes
        ftScanAndCompare(builder, "flintstone", 15, expected2);

        // part 3
        fullTextImpl.disableUpdateWorker();
        
        writeRow(c, 8, "Flintstone Hudson");
        writeRow(c, 9, "Jim Flintstone");
        
        // The worker has been disabled, waitOn should return immediately
        waitUpdate();

        // confirm that new rows are not found (ie., expected2 still works)
        ftScanAndCompare(builder, "flintstone", 15, expected2);

        fullTextImpl.enableUpdateWorker();
        waitUpdate();

        // now the rows should be seen.
        // (Because disabling the worker does not stop the changes fron being recorded)
        Row expected3[] = new Row[]
        {
            row(rowType, 1L),
            row(rowType, 3L),
            row(rowType, 5L),
            row(rowType, 7L),
            row(rowType, 8L),
            row(rowType, 9L)
        };

        ftScanAndCompare(builder, "flintstone", 15, expected3);
    }

    @Test
    public void cDown() throws InterruptedException {
        FullTextIndex index = createFullTextIndex(
                                                  SCHEMA, "c", "idx_c", 
                                                  "name", "i.sku", "a.state");
        RowType rowType = rowType("c");
        Row[] expected = new Row[] {
            row(rowType, 1L),
            row(rowType, 3L)
        };
        FullTextQueryBuilder builder = new FullTextQueryBuilder(index, ais(), queryContext);
        ftScanAndCompare(builder, "flintstone", 10, expected);
        ftScanAndCompare(builder, "state:MA", 10, expected);
    }

    @Test
    public void respondsToDropSchema() throws Exception {
        FullTextIndex index = createFullTextIndex(
                SCHEMA, "c", "idx_c",
                "name", "i.sku", "a.state");
        RowType rowType = rowType("c");
        Row[] expected = new Row[] {
                row(rowType, 1L),
                row(rowType, 3L)
        };
        FullTextQueryBuilder builder = new FullTextQueryBuilder(index, ais(), queryContext);
        ftScanAndCompare(builder, "flintstone", 10, expected);
        ftScanAndCompare(builder, "state:MA", 10, expected);
        ddl().dropSchema(session(), SCHEMA);
        c = createTable(SCHEMA, "c",
                "cid INT PRIMARY KEY NOT NULL",
                "name VARCHAR(128) COLLATE en_us_ci");
        index = createFullTextIndex(SCHEMA, "c", "idx_c", "name");
        expected = new Row[] {};
        builder = new FullTextQueryBuilder(index, ais(), queryContext);
        ftScanAndCompare(builder, "flintstone", 10, expected);
    }

    @Test
    public void oUpDown() throws InterruptedException {
        FullTextIndex index = createFullTextIndex(
                                                  SCHEMA, "o", "idx_o",
                                                  "c.name", "i.sku");
        RowType rowType = rowType("o");
        Row[] expected = new Row[] {
            row(rowType, 1L, 101L)
        };
        FullTextQueryBuilder builder = new FullTextQueryBuilder(index, ais(), queryContext);
        ftScanAndCompare(builder, "name:Flintstone AND sku:1234", 10, expected);
    }

    @Test
    public void testTruncate() throws InterruptedException {
        FullTextIndex index = createFullTextIndex(SCHEMA, "c", "idx_c", "name", "i.sku", "a.state");

        final int limit = 15;
        RowType rowType = rowType("c");
        String nameQuery = "flintstone";
        Row[] nameExpected = new Row[] { row(rowType, 1L), row(rowType, 3L) };
        String stateQuery = "state:MA";
        Row[] stateExpected = new Row[] { row(rowType, 1L), row(rowType, 3L) };
        String skuQuery = "sku:1234";
        Row[] skuExpected = new Row[] { row(rowType, 1L) };
        Row[] emptyExpected = {};
        
        FullTextQueryBuilder builder = new FullTextQueryBuilder(index, ais(), queryContext);
        ftScanAndCompare(builder, nameQuery, limit, nameExpected);
        ftScanAndCompare(builder, stateQuery, limit, stateExpected);
        ftScanAndCompare(builder, skuQuery, limit, skuExpected);

        dml().truncateTable(session(), a);
        waitUpdate();
        ftScanAndCompare(builder, nameQuery, limit, nameExpected);
        ftScanAndCompare(builder, stateQuery, limit, emptyExpected);
        ftScanAndCompare(builder, skuQuery, limit, skuExpected);

        dml().truncateTable(session(), o);
        waitUpdate();
        ftScanAndCompare(builder, nameQuery, limit, nameExpected);
        ftScanAndCompare(builder, stateQuery, limit, emptyExpected);
        ftScanAndCompare(builder, skuQuery, limit, emptyExpected); // Non-cascading key, connection to c1 is unknown

        dml().truncateTable(session(), i);
        waitUpdate();
        ftScanAndCompare(builder, nameQuery, limit, nameExpected);
        ftScanAndCompare(builder, stateQuery, limit, emptyExpected);
        ftScanAndCompare(builder, skuQuery, limit, emptyExpected);

        dml().truncateTable(session(), c);
        waitUpdate();
        ftScanAndCompare(builder, nameQuery, limit, emptyExpected);
        ftScanAndCompare(builder, stateQuery, limit, emptyExpected);
        ftScanAndCompare(builder, skuQuery, limit, emptyExpected);
    }


    protected RowType rowType(String tableName) {
        return schema.newHKeyRowType(ais().getTable(SCHEMA, tableName).hKey());
    }

    protected TestRow row(RowType rowType, Object... fields) {
        return new TestRow(rowType, fields);
    }

    private void ftScanAndCompare(FullTextQueryBuilder builder, String query, int limit, Row[] expected) {
        try(CloseableTransaction txn = txnService().beginCloseableTransaction(session())) {
            Operator plan = builder.scanOperator(query, limit);
            compareRows(expected, cursor(plan, queryContext, queryBindings));
            txn.commit();
        }
    }
}
