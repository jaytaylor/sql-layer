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

package com.akiban.server.mttests.mthapi.basiccoi;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.TableName;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.api.HapiGetRequest;
import com.akiban.server.api.HapiPredicate;
import com.akiban.server.api.HapiRequestException;
import com.akiban.server.mttests.mthapi.base.HapiMTBase;
import com.akiban.server.mttests.mthapi.base.HapiSuccess;
import com.akiban.server.mttests.mthapi.base.WriteThread;
import com.akiban.server.service.memcache.ParsedHapiGetRequest;
import com.akiban.server.service.memcache.SimpleHapiPredicate;
import com.akiban.server.service.session.Session;
import com.akiban.util.Strings;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

import static com.akiban.util.ThreadlessRandom.rand;
import static org.junit.Assert.assertEquals;

public final class CoiMT extends HapiMTBase {

    private abstract static class Writer implements WriteThread {
        private Integer customer;
        private Integer order;
        private Integer item;

        @Override
        public final void setupWrites(DDLFunctions ddl, DMLFunctions dml, Session session)
                throws InvalidOperationException
        {
            ddl.createTable(session, "s1", "create table c(id int key, age int)");
            ddl.createTable(session, "s1", "create table o(id int key, cid int, "
                    +" CONSTRAINT __akiban_o FOREIGN KEY __akiban_o (cid) REFERENCES c (id)"
                    +" )");
            ddl.createTable(session, "s1", "create table i(id int key, oid int, "
                    +" CONSTRAINT __akiban_i FOREIGN KEY __akiban_o (oid) REFERENCES o (id)"
                    +" )");

            customer = ddl.getTableId(session, new TableName("s1", "c") );
            order = ddl.getTableId(session, new TableName("s1", "o") );
            item = ddl.getTableId(session, new TableName("s1", "i") );

            setupRows(session, dml);
        }

        protected abstract void setupRows(Session session, DMLFunctions dml) throws InvalidOperationException;

        protected final int customers() {
            return customer;
        }

        protected final int orders() {
            return order;
        }

        protected final int items() {
            return item;
        }
    }

    @Test
    public void allWritesFirst() throws HapiRequestException, JSONException, IOException {
        WriteThread writeThread = new Writer() {
            @Override
            protected void setupRows(Session session, DMLFunctions dml) throws InvalidOperationException {
                dml.writeRow( session, createNewRow(customers(), 1, 27) );

                dml.writeRow( session, createNewRow(orders(), 1, 1) );
                dml.writeRow( session, createNewRow(orders(), 2, 1) );

                dml.writeRow( session, createNewRow(items(), 1, 1) );
            }

            @Override
            public void ongoingWrites(DDLFunctions ddl, DMLFunctions dml, Session session, AtomicBoolean keepGoing)
                    throws InvalidOperationException
            {
                // no ongoing writes
            }
        };

        final HapiGetRequest request = ParsedHapiGetRequest.parse("s1:c:id=1");
        JSONObject expectedJSON = new JSONObject(
                Strings.join(Strings.dumpResource(CoiMT.class, "allWritesFirst_expected.json"))
        );
        final String expectedResponse = expectedJSON.toString(4);

        HapiSuccess readThread = new HapiSuccess() {
            @Override
            protected void validateSuccessResponse(HapiGetRequest request, JSONObject result)
                    throws JSONException
            {
                assertEquals(request.toString(), expectedResponse, result.toString(4));
            }

            @Override
            protected void validateIndex(HapiGetRequest request, Index index) {
                assertTrue("index table: " + index, index.getTableName().equals("s1", "c"));
                assertEquals("index name", "PRIMARY", index.getIndexName().getName());
            }

            @Override
            protected HapiGetRequest pullRequest(int pseudoRandom) {
                return request;
            }

            @Override
            protected int spawnCount() {
                return 2500;
            }
        };

        runThreads(writeThread, readThread);
    }

    @Test
    public void concurrentWritesWithOrphans() throws HapiRequestException, JSONException, IOException {
        final int MAX_INT = 100;
        final int MAX_INC = 10;
        final int MAX_READ_ID = 1000;
        WriteThread writeThread = new Writer() {
            @Override
            protected void setupRows(Session session, DMLFunctions dml) throws InvalidOperationException {
                final int[] tables = {customers(), orders(), items()};
                long start = System.currentTimeMillis();
                int seed = (int)start;
                while (System.currentTimeMillis() - start > 5000) {
                    seed = writeRandomly(session, seed, tables, new int[]{1, 1, 1}, dml);
                }
            }

            @Override
            public void ongoingWrites(DDLFunctions ddl, DMLFunctions dml, Session session, AtomicBoolean keepGoing)
                    throws InvalidOperationException
            {
                final int[] tables = {customers(), orders(), items()};
                final int[] tableFirstCols = {1, 1, 1};
                int seed = this.hashCode();

                while (keepGoing.get()) {
                    seed = writeRandomly(session, seed, tables, tableFirstCols, dml);
                }
            }

            private int writeRandomly(Session session, int seed, int[] tables, int[] tableFirstCols, DMLFunctions dml)
                    throws InvalidOperationException
            {
                seed = rand(seed);
                final int tableIndex = Math.abs(seed % 3);
                final int tableId = tables[ tableIndex ];
                tableFirstCols[tableIndex] += Math.abs(seed % MAX_INC) + 1;
                seed = rand(seed);
                final int secondInt = seed % MAX_INT;
                dml.writeRow(session, createNewRow(tableId, tableFirstCols[tableIndex], secondInt));
                return seed;
            }
        };

        HapiSuccess readThread = new HapiSuccess() {

            @Override
            protected void validateIndex(HapiGetRequest request, Index index) {
                assertTrue("index table: " + index, index.getTableName().equals("s1", "c"));
                assertEquals("index name", "PRIMARY", index.getIndexName().getName());
            }

            @Override
            protected void validateSuccessResponse(HapiGetRequest request, JSONObject result)
                    throws JSONException
            {
                JSONArray customers = result.getJSONArray("@c");

                int customersCount = customers.length();
                assertFalse(String.format("too many customsers (%d): %s -> %s", customersCount, request, result),
                        customersCount > 1);

                if (customers.length() > 0) {
                    JSONObject customer = customers.getJSONObject(0);
                    Set<String> cKeys = jsonObjectKeys(customer);
                    assertEquals(cKeys + " length", 3, cKeys.size());
                    final int cID = jsonObjectInt(customer, "id", request);
                    assertEquals("customer id", request.getPredicates().get(0).getValue(), Integer.toString(cID));
                    assertTrue("customer missing age: " + result, cKeys.contains("age"));
                    JSONArray orders = customer.getJSONArray("@o");

                    for (int ordersLen=orders.length(), oIndex=0; oIndex < ordersLen; ++oIndex ) {
                        JSONObject order = orders.getJSONObject(oIndex);
                        Set<String> oKeys = jsonObjectKeys(order);
                        assertEquals(oKeys + " length", 3, oKeys.size());
                        final int oID = jsonObjectInt(order, "id", request);
                        assertEquals("cid", cID, jsonObjectInt(order, "cid", request));
                        JSONArray items = order.getJSONArray("@i");


                        for (int itemsLen=items.length(), iIndex=0; iIndex < itemsLen; ++iIndex) {
                            JSONObject item = items.getJSONObject(iIndex);
                            Set<String> iKeys = jsonObjectKeys(item);
                            assertEquals(iKeys + " length", 2, iKeys.size());
                            assertTrue("item lacking id: " + result, iKeys.contains("id"));
                            assertEquals("item's order", oID, jsonObjectInt(item, "oid", request));
                        }
                    }
                }
            }

            @Override
            protected HapiGetRequest pullRequest(final int pseudoRandom) {
                return new HapiGetRequest() {
                    private final String idValue = Integer.toString(Math.abs(pseudoRandom) % MAX_READ_ID);
                    private final TableName using = new TableName("s1", "c");
                    @Override
                    public String getSchema() {
                        return using.getSchemaName();
                    }

                    @Override
                    public String getTable() {
                        return using.getTableName();
                    }

                    @Override
                    public TableName getUsingTable() {
                        return using;
                    }

                    @Override
                    public List<HapiPredicate> getPredicates() {
                        return Arrays.<HapiPredicate>asList(
                                new SimpleHapiPredicate(using, "id", HapiPredicate.Operator.EQ, idValue)
                        );
                    }

                    @Override
                    public String toString() {
                        return String.format("%s:%s:%s=%s", getSchema(), getTable(), "id", idValue);
                    }
                };
            }

            @Override
            protected int spawnCount() {
                return 10000;
            }
        };

        runThreads(writeThread, readThread);
    }

    private Set<String> jsonObjectKeys(JSONObject jsonObject) {
        Iterator iter = jsonObject.keys();
        Set<String> keys = new HashSet<String>();
        while (iter.hasNext()) {
            String key = (String) iter.next();
            if (!keys.add(key)) {
                fail(String.format("dupliate key %s in %s", key, jsonObject));
            }
        }
        return keys;
    }

    private int jsonObjectInt(JSONObject jsonObject, String key, HapiGetRequest request) {
        assertFalse("<" + request + "> " + key + " null: " + jsonObject, jsonObject.isNull(key));
        try {
            return jsonObject.getInt(key);
        } catch (JSONException e) {
            throw new RuntimeException("<" + request + "> extracting " + key + " from " + jsonObject.toString(), e);
        }
    }
}
