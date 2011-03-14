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

package com.akiban.server.mttests.mthapi.ddlandhapi;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.TableName;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.api.HapiGetRequest;
import com.akiban.server.api.HapiRequestException;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.hapi.DefaultHapiGetRequest;
import com.akiban.server.mttests.mthapi.base.HapiMTBase;
import com.akiban.server.mttests.mthapi.base.HapiReadThread;
import com.akiban.server.mttests.mthapi.base.HapiRequestStruct;
import com.akiban.server.mttests.mthapi.base.HapiSuccess;
import com.akiban.server.mttests.mthapi.base.WriteThread;
import com.akiban.server.mttests.mthapi.base.WriteThreadStats;
import com.akiban.server.mttests.mthapi.base.sais.SaisBuilder;
import com.akiban.server.mttests.mthapi.base.sais.SaisFK;
import com.akiban.server.mttests.mthapi.base.sais.SaisTable;
import com.akiban.server.mttests.mthapi.common.DDLUtils;
import com.akiban.server.mttests.mthapi.common.HapiValidationError;
import com.akiban.server.service.session.Session;
import com.akiban.util.ThreadlessRandom;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public final class AddDropTableMT extends HapiMTBase {
    private static final Logger LOG = LoggerFactory.getLogger(AddDropTableMT.class);
    private static final String SCHEMA = "indexestest";

    @Test
    public void addDropIndex() {
        WriteThread writeThread = getAddDropTableThread();

        runThreads(writeThread,
//                readThread("aString", 200, false, .4f),
//                readThread("anInt", 200, true, .4f),
//                readThread("id", 100, false, .2f)
                new HapiSuccess() {
                    @Override
                    protected void validateSuccessResponse(HapiRequestStruct request, JSONObject result) throws Exception {
                        throw new UnsupportedOperationException(); // TODO
                    }

                    @Override
                    protected HapiRequestStruct pullRequest(int pseudoRandom) {
                        while (true) {
                            try {
                                Thread.sleep(10 * 60 * 1000);
                            } catch (InterruptedException e) {
                                e.printStackTrace();  // TODO
                            }
                        }
                    }

                    @Override
                    protected void validateIndex(HapiRequestStruct request, Index queriedIndex) {
                        throw new UnsupportedOperationException(); // TODO
                    }
                }
        );
    }

    private HapiReadThread readThread(final String column, final int max, final boolean reverse, final float chance) {
        SaisBuilder builder = new SaisBuilder();
        builder.table("p", "id", "aString", "anInt").pk("id");
        builder.table("c1", "id", "pid").pk("id").joinTo("p").col("id", "pid");
        final SaisTable pTable = builder.getSoleRootTable();
        return new OptionallyWorkingReadThread(SCHEMA, pTable, HapiRequestException.ReasonCode.UNSUPPORTED_REQUEST) {

            @Override
            protected HapiRequestStruct pullRequest(int pseudoRandom) {
                int id = (Math.abs(pseudoRandom) % (max-1)) + 1;
                if (reverse)  {
                    id = -id;
                }
                HapiGetRequest request = DefaultHapiGetRequest.forTables(SCHEMA, "p", "p")
                        .withEqualities(column, Integer.toString(id)).done();
                return new HapiRequestStruct(request, pTable, null);
            }

            @Override
            protected void validateSuccessResponse(HapiRequestStruct requestStruct, JSONObject result) throws JSONException {
                super.validateSuccessResponse(requestStruct, result);
                HapiValidationError.assertFalse(HapiValidationError.Reason.ROOT_TABLES_COUNT,
                        "more than one root found",
                        result.getJSONArray("@p").length() > 1);
                // Also, we must have results!
//                TODO: this isn't a valid test while we allow concurrent scans and adding/dropping of indexes
//                see: https://answers.launchpad.net/akiban-server/+question/148857
//                HapiValidationError.assertEquals(HapiValidationError.Reason.ROOT_TABLES_COUNT,
//                        "number of roots",
//                        1, result.getJSONArray("@p").length()
//                );
            }

            @Override
            protected int spawnCount() {
                float spawnRoughly = chance * super.spawnCount();
                return 1000 * (int)(spawnRoughly + .5);
            }
        };
    }

    private WriteThread getAddDropTableThread() {
        SaisBuilder builder = new SaisBuilder();

        builder.table("customers", "cid", "age")
                .pk("cid");

        builder.table("orders", "oid", "c_id", "priority")
                .pk("oid", "c_id")
                .key("priority")
                .joinTo("customers").col("cid", "c_id");

        builder.table("items", "iid", "o_id", "c_id")
                .joinTo("orders").col("oid", "o_id").col("c_id", "c_id");

        builder.table("addresses","aid", "c_id")
                .pk("aid")
                .joinTo("customers").col("cid", "c_id");

        final SaisTable customer = builder.getSoleRootTable();
        return new AddDropTablesWriter(customer) {
            private final int CUSTOMERS_COUNT = 100;
            private final int ORDERS_PER_CUSTOMER = 3;
            private final int ITEMS_PER_ORDER = 3;
            private final int ADDRESSES_PER_CUSTOMER = 2;

            @Override
            protected void writeTableRows(Session session, DMLFunctions dml, int tableId, SaisTable table)
                    throws InvalidOperationException
            {
                if ("customers".equals(table.getName())) {
                    customers(dml, session, tableId);
                }
                else if ("orders".equals(table.getName())) {
                    orders(dml, session, tableId);
                }
                else if ("items".equals(table.getName())) {
                    items(dml, session, tableId);
                }
                else if ("addresses".equals(table.getName())) {
                    addresses(dml, session, tableId);
                }
            }

            private void customers(DMLFunctions dml, Session session, int tableId) throws InvalidOperationException {
                for (int cid = 0; cid < CUSTOMERS_COUNT; ++cid) {
                    NewRow row = createNewRow(tableId, cid, age(cid));
                    dml.writeRow(session, row);
                }
            }

            private void orders(DMLFunctions dml, Session session, int tableId) throws InvalidOperationException {
                for (int cid = 0; cid < CUSTOMERS_COUNT; ++cid) {
                    for (int oid = 0; oid < ORDERS_PER_CUSTOMER; ++oid) {
                        NewRow row = createNewRow(tableId, cid, oid, priority(cid, oid));
                        dml.writeRow(session, row);
                    }
                }
            }

            private void items(DMLFunctions dml, Session session, int tableId) throws InvalidOperationException {
                int iid = 0;
                for (int cid = 0; cid < CUSTOMERS_COUNT; ++cid) {
                    for (int oid = 0; oid < ORDERS_PER_CUSTOMER; ++oid) {
                        for (int count=0; count < ITEMS_PER_ORDER; ++count) {
                            NewRow row = createNewRow(tableId, iid, oid, cid);
                            dml.writeRow(session, row);
                            --iid;
                        }
                    }
                }
            }

            private void addresses(DMLFunctions dml, Session session, int tableId) throws InvalidOperationException {
                for (int cid = 0; cid < CUSTOMERS_COUNT; ++cid) {
                    for (int count=0; count < ADDRESSES_PER_CUSTOMER; ++count) {
                        NewRow row = createNewRow(tableId, aid(cid, count), cid);
                        dml.writeRow(session, row);
                    }
                }
            }

            private int age(int cid) {
                return cid * 2;
            }

            private int priority(int oid, int cid) {
                return cid % 2 == 0 ? oid : -oid;
            }

            private int aid(int cid, int count) {
                assert count >= 0 && count < 10 : count;
                return (cid * 10) + count;
            }
        };
    }

    private abstract static class AddDropTablesWriter implements WriteThread {

        /**
         * All of the tables on which we can perform an action (adding or dropping). The table's value corresponds
         * to its table ID, or null if the table isn't in the system.
         */
        private final Map<SaisTable,Integer> tablesMap;
        /**
         * A list form of the actionableTablesMap's keys. Used for randomly selecting one.
         */
        private final List<SaisTable> tablesList;
        private final ThreadlessRandom rand = new ThreadlessRandom();

        AddDropTablesWriter(SaisTable root) {
            tablesMap = new HashMap<SaisTable, Integer>();
            tablesList = new ArrayList<SaisTable>();
            putTable(root, null, true);
        }

        private void putTable(SaisTable table, Integer tableId, boolean override) {
            boolean alreadyContains = tablesMap.containsKey(table);
            if (!override && alreadyContains) {
                return;
            }
            Integer old = tablesMap.put(table, tableId);
            if (!override) {
                assert old == null || old.equals(tableId): String.format("%s -> %s conflicts with %s", table, old, tableId);
            }
            if (!alreadyContains) {
                tablesList.add(table);
            }
        }

        private void removeTable(SaisTable table) {
            tablesMap.remove(table);
            boolean removeWorked = tablesList.remove(table);
            assert removeWorked : table;
        }

        @Override
        public void setupWrites(DDLFunctions ddl, DMLFunctions dml, Session session) {
            // nothing
        }

        @Override
        public void ongoingWrites(DDLFunctions ddl, DMLFunctions dml, Session session, AtomicBoolean keepGoing)
                throws InvalidOperationException
        {
            // pick a table to do something to
            SaisTable table = tablesList.get( rand.nextInt(0, tablesList.size()) );
            Integer tableId = tablesMap.get(table);
            if (tableId == null) {
                LOG.trace("about to add table {} with id {}", table, tableId);
                tableId = addTable(session, ddl, table);
                writeTableRows(session, dml, tableId, table);
                LOG.trace("added table {} (with rows)", table);
            }
            else {
                LOG.trace("about to drop table {} with id {}", table, tableId);
                dropTable(session, ddl, table);
                LOG.trace("dropped table {}", table);
            }
        }

        private int addTable(Session session, DDLFunctions ddl, SaisTable table) throws InvalidOperationException {
            String ddlText = DDLUtils.buildDDL(table);
            ddl.createTable(session, SCHEMA, ddlText);
            int tableId = ddl.getTableId(session, new TableName(SCHEMA, table.getName()));
            if (table.getChildren().isEmpty()) {
                putTable(table, tableId, true);
            }
            else {
                removeTable(table);
                for (SaisFK childFK : table.getChildren()) {
                    putTable(childFK.getChild(), null, true);
                }
            }
            return tableId;
        }


        protected abstract void writeTableRows(Session session, DMLFunctions dml, int tableId, SaisTable table)
                throws InvalidOperationException;

        private void dropTable(Session session, DDLFunctions ddl, SaisTable table) throws InvalidOperationException {
            ddl.dropTable(session, new TableName(SCHEMA, table.getName()));
            // Drop the children
            for (SaisFK childFK : table.getChildren()) {
                removeTable(childFK.getChild());
            }

            // Add the siblings (including this table) if they're not there
            SaisFK parentFK = table.getParentFK();
            if (parentFK == null) {
                putTable(table, null, true);
            }
            else {
                for (SaisFK siblingFK : parentFK.getParent().getChildren()) {
                    SaisTable sibling = siblingFK.getChild();
                    putTable(sibling, null, sibling.equals(table)); // for the original table only, override the null
                }
            }
        }

        @Override
        public WriteThreadStats getStats() {
            return new WriteThreadStats(0, 0, 0);
        }

        @Override
        public boolean continueThroughException(Throwable throwable) {
            throwable.printStackTrace();
            return true;
        }
    }
}
