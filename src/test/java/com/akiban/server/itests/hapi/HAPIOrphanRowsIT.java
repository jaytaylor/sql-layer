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

package com.akiban.server.itests.hapi;

import com.akiban.server.InvalidOperationException;
import com.akiban.server.RowData;
import com.akiban.server.api.HapiGetRequest;
import com.akiban.server.api.HapiOutputter;
import com.akiban.server.api.HapiProcessedGetRequest;
import com.akiban.server.api.HapiRequestException;
import com.akiban.server.api.dml.scan.LegacyRowWrapper;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.itests.ApiTestBase;
import com.akiban.server.service.memcache.ParsedHapiGetRequest;
import com.akiban.server.service.memcache.hprocessor.DefaultProcessedRequest;
import com.akiban.server.service.memcache.hprocessor.Scanrows;
import com.akiban.server.service.memcache.outputter.jsonoutputter.JsonOutputter;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

import static org.junit.Assert.*;

public class HAPIOrphanRowsIT extends ApiTestBase
{
    @Test
    public void testRepeatedly() throws Exception
    {
        for (int t = 0; t < TRIALS; t++) {
            test(t);
            dropAllTables();
        }
    }

    public void test(int trial) throws Exception
    {
        random = new Random(SEED + trial);
        createSchema();
        populateDatabase();
        runQueries();
    }

    private void createSchema() throws InvalidOperationException
    {
        customerTable = createTable(
            SCHEMA, "customer",
            "cid int not null",
            "cid_copy int not null",
            "primary key(cid)");
        orderTable = createTable(
            SCHEMA, "order",
            "cid int not null",
            "oid int not null",
            "cid_copy int not null",
            "primary key(cid, oid)",
            "index idx_cid_copy(cid_copy)",
            "constraint __akiban_oc foreign key __akiban_oc(cid) references customer(cid)");
        itemTable = createTable(
            SCHEMA, "item",
            "cid int not null",
            "oid int not null",
            "iid int not null",
            "cid_copy int not null",
            "primary key(cid, oid, iid)",
            "index idx_cid_copy(cid_copy)",
            "constraint __akiban_io foreign key __akiban_io(cid, oid) references order(cid, oid)");
        addressTable = createTable(
            SCHEMA, "address",
            "cid int not null",
            "aid int not null",
            "cid_copy int not null",
            "primary key(cid, aid)",
            "index idx_cid_copy(cid_copy)",
            "constraint __akiban_ac foreign key __akiban_ac(cid) references customer(cid)");
    }

    private void populateDatabase() throws Exception
    {
        db.clear();
        // print("------------------------------------------");
        Sampler sampler;
        int nCustomer = random.nextInt(MAX_CUSTOMERS + 1);
        sampler = new Sampler(nCustomer);
        for (int i = 0; i < nCustomer; i++) {
            long cid = sampler.take();
            NewRow row = createNewRow(customerTable, cid, cid);
            // print("%s", cid);
            addRow(row);
        }
        int nOrder = random.nextInt(MAX_CUSTOMERS * MAX_ORDERS_PER_CUSTOMER + 1);
        sampler = new Sampler(nOrder);
        for (int i = 0; i < nOrder; i++) {
            long cidOid = sampler.take();
            long cid = cidOid / MAX_ORDERS_PER_CUSTOMER;
            long oid = cidOid % MAX_ORDERS_PER_CUSTOMER;
            NewRow row = createNewRow(orderTable, cid, oid, cid);
            // print("%s %s", cid, oid);
            addRow(row);
        }
        int nItem = random.nextInt(MAX_CUSTOMERS * MAX_ORDERS_PER_CUSTOMER * MAX_ITEMS_PER_ORDER + 1);
        sampler = new Sampler(nItem);
        for (int i = 0; i < nItem; i++) {
            long cidOidIid = sampler.take();
            long cid = cidOidIid / (MAX_ORDERS_PER_CUSTOMER * MAX_ITEMS_PER_ORDER);
            long oid = (cidOidIid / MAX_ITEMS_PER_ORDER) % MAX_ORDERS_PER_CUSTOMER;
            long iid = cidOidIid % MAX_ITEMS_PER_ORDER;
            NewRow row = createNewRow(itemTable, cid, oid, iid, cid);
            // print("%s %s %s", cid, oid, iid);
            addRow(row);
        }
        int nAddress = random.nextInt(MAX_CUSTOMERS * MAX_ADDRESSES_PER_CUSTOMER + 1);
        sampler = new Sampler(nAddress);
        for (int i = 0; i < nAddress; i++) {
            long cidAid = sampler.take();
            long cid = cidAid / MAX_ADDRESSES_PER_CUSTOMER;
            long aid = cidAid % MAX_ADDRESSES_PER_CUSTOMER;
            NewRow row = createNewRow(addressTable, cid, aid, cid);
            // print("%s %s", cid, aid);
            addRow(row);
        }
        sort(db);
    }

    private void runQueries() throws Exception
    {
        for (int cid = 0; cid < MAX_CUSTOMERS; cid++) {
            for (Comparison comparison : Comparison.values()) {
                print("cid %s %s", comparison, cid);
                runQuery(customerTable, customerTable, Column.C_CID, comparison, cid);
            }
        }
    }

    private void runQuery(int rootTable,
                          int predicateTable,
                          Column predicateColumn,
                          Comparison comparison,
                          int literal) throws Exception
    {
/*
        for (NewRow row : db) {
            print(row.toString());
        }
        print("----------------------------------------------------");
*/
        assertEquals(rootTable, predicateTable); // for now
        String actual = actualQueryResult(rootTable, predicateTable, predicateColumn, comparison, literal);
        String expected = expectedQueryResult(rootTable, predicateTable, predicateColumn, comparison, literal);
        assertEquals(expected, actual);
    }

    private String actualQueryResult(int rootTable,
                                     int predicateTable,
                                     Column predicateColumn,
                                     Comparison comparison,
                                     int literal) throws HapiRequestException
    {
        String query = hapiQuery(rootTable, predicateTable, predicateColumn, comparison, literal);
        request = ParsedHapiGetRequest.parse(query);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(10000);
        Scanrows.instance().processRequest(session, request, outputter, outputStream);
        return new String(outputStream.toByteArray());
    }

    private String expectedQueryResult(int rootTable,
                                       int predicateTable,
                                       Column predicateColumn,
                                       Comparison comparison,
                                       long literal) throws HapiRequestException, IOException
    {
        List<NewRow> queryResult = new ArrayList<NewRow>();
        // Find rows belonging to predicate table
        for (NewRow row : db) {
            if (row.getTableId() == predicateTable &&
                evaluate(row, predicateTable, predicateColumn, comparison, literal)) {
                queryResult.add(row);
            }
        }
        // Fill in descendents
        List<NewRow> descendents = new ArrayList<NewRow>();
        for (NewRow resultRow : queryResult) {
            for (NewRow row : db) {
                if (ancestorOf(resultRow, row)) {
                    descendents.add(row);
                }
            }
        }
        queryResult.addAll(descendents);
        // Sort
        sort(queryResult);
        // Convert to RowData, filling in hkey segment at which each row differs from its predecessor
        List<RowData> rowDatas = new ArrayList<RowData>();
        NewRow previousRow = null;
        for (NewRow row : queryResult) {
            int differSegment = 0;
            if (previousRow != null) {
                differSegment = hKey(previousRow).differSegment(hKey(row));
                assertTrue(differSegment > 0);
            }
            RowData rowData = row.toRowData();
            rowData.differsFromPredecessorAtKeySegment(differSegment);
            rowDatas.add(rowData);
            previousRow = row;
        }
        // Generate json string
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(10000);
        outputter.output(new DefaultProcessedRequest(request, session, ddl()),
                         rowDatas,
                         outputStream);
        return new String(outputStream.toByteArray());
    }

    private boolean ancestorOf(NewRow x, NewRow y)
    {
        boolean ancestor = false;
        int xType = x.getTableId();
        int yType = y.getTableId();
        if (xType == customerTable) {
            if (yType == orderTable) {
                // Compare cids
                ancestor = y.get(0).equals(x.get(0));
            } else if (yType == itemTable) {
                // Compare cids
                ancestor = y.get(0).equals(x.get(0));
            } else if (yType == addressTable) {
                // Compare cids
                ancestor = y.get(0).equals(x.get(0));
            }
        } else if (xType == orderTable) {
            if (yType == itemTable) {
                // Compare cids and oids
                ancestor = y.get(0).equals(x.get(0)) && y.get(1).equals(x.get(1));
            }
        }
        return ancestor;
    }

    private boolean evaluate(NewRow row,
                             int predicateTable,
                             Column predicateColumn,
                             Comparison comparison,
                             long literal)
    {
        return evaluate(columnValue(row, predicateColumn), comparison, literal);
    }

    private long columnValue(NewRow row, Column column)
    {
        int columnPosition = -1;
        int tableId = row.getTableId();
        if (tableId == customerTable) {
            columnPosition = 0;
        } else if (tableId == orderTable) {
            String columnName = column.columnName();
            if (columnName.equals("cid")) {
                columnPosition = 0;
            } else if (columnName.equals("oid")) {
                columnPosition = 1;
            } else if (columnName.equals("cid_copy")) {
                columnPosition = 2;
            }
        } else if (tableId == itemTable) {
            String columnName = column.columnName();
            if (columnName.equals("cid")) {
                columnPosition = 0;
            } else if (columnName.equals("oid")) {
                columnPosition = 1;
            } else if (columnName.equals("iid")) {
                columnPosition = 2;
            } else if (columnName.equals("cid_copy")) {
                columnPosition = 3;
            }
        } else if (tableId == addressTable) {
            String columnName = column.columnName();
            if (columnName.equals("cid")) {
                columnPosition = 0;
            } else if (columnName.equals("aid")) {
                columnPosition = 1;
            } else if (columnName.equals("cid_copy")) {
                columnPosition = 2;
            }
        } else {
            fail();
        }
        return (Long) row.get(columnPosition);
    }

    private boolean evaluate(long columnValue, Comparison comparison, long literal)
    {
        switch (comparison) {
            case LT:
                return columnValue < literal;
            case LE:
                return columnValue <= literal;
            case GT:
                return columnValue > literal;
            case GE:
                return columnValue >= literal;
            case EQ:
                return columnValue == literal;
        }
        fail();
        return false;
    }

    private String hapiQuery(int rootTable,
                             int predicateTable,
                             Column predicateColumn,
                             Comparison comparison,
                             int literal)
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append(SCHEMA);
        buffer.append(COLON);
        if (rootTable == predicateTable) {
            buffer.append(tableName(predicateTable).getTableName());
            buffer.append(COLON);
        } else {
            buffer.append(tableName(rootTable).getTableName());
            buffer.append(OPEN);
            buffer.append(tableName(predicateTable).getTableName());
            buffer.append(CLOSE);
        }
        buffer.append(predicateColumn.columnName());
        buffer.append(comparison);
        buffer.append(literal);
        return buffer.toString();
    }

    private void addRow(NewRow row) throws Exception
    {
        dml().writeRow(session, row);
        db.add(row);
    }

    private HKey hKey(NewRow row)
    {
        return new HKey(row);
    }

    private void sort(List<NewRow> rows)
    {
        Collections.sort(rows, ROW_COMPARATOR);
    }

    private void print(String template, Object... args)
    {
        System.out.println(String.format(template, args));
    }

    private String formatJSON(String json) throws JSONException
    {
        return new JSONObject(json).toString(4);
    }

    // Schema elements
    enum Column
    {
        // Customer
        C_CID("cid"),
        // Order
        O_CID("cid"),
        O_OID("oid"),
        O_CID_COPY("cid_copy"),
        // Item
        I_CID("cid"),
        I_OID("oid"),
        I_IID("iid"),
        I_CID_COPY("cid_copy"),
        // Address
        A_CID("cid"),
        A_AID("aid"),
        A_CID_COPY("cid_copy");

        public String columnName()
        {
            return name;
        }

        private Column(String name)
        {
            this.name = name;
        }

        private String name;
    }

    // Comparison operators
    enum Comparison
    {
        LT("<"),
        LE("<="),
        GT(">"),
        GE(">="),
        EQ("=");

        public String toString()
        {
            return symbol;
        }

        private Comparison(String symbol)
        {
            this.symbol = symbol;
        }

        private String symbol;
    }

    Comparator<NewRow> ROW_COMPARATOR = new Comparator<NewRow>()
    {
        @Override
        public int compare(NewRow x, NewRow y)
        {
            return hKey(x).compareTo(hKey(y));
        }
    };
    // Test parameters
    private static final int TRIALS = 20;
    private static final int SEED = 123456789;
    private static final int MAX_CUSTOMERS = 3;
    private static final int MAX_ORDERS_PER_CUSTOMER = 3;
    private static final int MAX_ITEMS_PER_ORDER = 2;
    private static final int MAX_ADDRESSES_PER_CUSTOMER = 2;
    // Constants
    private static final String SCHEMA = "schema";
    private static final String COLON = ":";
    private static final String OPEN = "(";
    private static final String CLOSE = ")";

    private Random random;
    private int customerTable;
    private int orderTable;
    private int itemTable;
    private int addressTable;
    private List<NewRow> db = new ArrayList<NewRow>();
    private final JsonOutputter outputter = new JsonOutputter();
    private HapiGetRequest request;

    // For sampling without replacement
    private class Sampler
    {
        public int take()
        {
            int position = random.nextInt(ids.size());
            int id = ids.get(position);
            ids.remove(position);
            return id;
        }

        public Sampler(int n)
        {
            ids = new ArrayList<Integer>(n);
            for (int i = 0; i < n; i++) {
                ids.add(i);
            }
        }

        private final List<Integer> ids;
    }

    private class TestOutputter implements HapiOutputter
    {
        @Override
        public void output(HapiProcessedGetRequest request, Iterable<RowData> rowDatas, OutputStream outputStream) throws IOException
        {
            for (RowData rowData : rowDatas) {
                rows.add(new LegacyRowWrapper(rowData));
            }
        }

        public List<NewRow> rows()
        {
            return rows;
        }

        private final List<NewRow> rows = new ArrayList<NewRow>();
    }

    private class HKey implements Comparable<HKey>
    {
        @Override
        public int compareTo(HKey that)
        {
            int n = Math.min(this.key.length, that.key.length);
            for (int i = 0; i < n; i++) {
                long c = this.key[i] - that.key[i];
                if (c != 0) {
                    return (int) c;
                }
            }
            return (this.key.length - that.key.length);
        }

        public HKey(NewRow row)
        {
            int tableId = row.getTableId();
            if (tableId == customerTable) {
                key = new long[]{
                    customerTable, (Long) row.get(0)
                };
            } else if (tableId == orderTable) {
                key = new long[]{
                    customerTable, (Long) row.get(0),
                    orderTable, (Long) row.get(1)
                };
            } else if (tableId == itemTable) {
                key = new long[]{
                    customerTable, (Long) row.get(0),
                    orderTable, (Long) row.get(1),
                    itemTable, (Long) row.get(2)
                };
            } else if (tableId == addressTable) {
                key = new long[]{
                    customerTable, (Long) row.get(0),
                    addressTable, (Long) row.get(1)
                };
            } else {
                key = null;
                fail();
            }
        }

        public int differSegment(HKey that)
        {
            int differSegment;
            int nx = this.key.length;
            int ny = that.key.length;
            int n = Math.min(nx, ny);
            for (differSegment = 0; differSegment < n; differSegment++) {
                if (this.key[differSegment] != that.key[differSegment]) {
                    break;
                }
            }
            return differSegment;
        }

        private final long[] key;
    }
}
