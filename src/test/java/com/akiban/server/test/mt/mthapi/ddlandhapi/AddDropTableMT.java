/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.test.mt.mthapi.ddlandhapi;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.TableName;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.api.HapiGetRequest;
import com.akiban.server.api.HapiRequestException;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.hapi.DefaultHapiGetRequest;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.OldAISException;
import com.akiban.server.error.TableDefinitionChangedException;
import com.akiban.server.rowdata.SchemaFactory;
import com.akiban.server.test.mt.mthapi.base.HapiMTBase;
import com.akiban.server.test.mt.mthapi.base.HapiRequestStruct;
import com.akiban.server.test.mt.mthapi.base.WriteThread;
import com.akiban.server.test.mt.mthapi.base.sais.SaisBuilder;
import com.akiban.server.test.mt.mthapi.base.sais.SaisFK;
import com.akiban.server.test.mt.mthapi.base.sais.SaisTable;
import com.akiban.server.test.mt.mthapi.common.DDLUtils;
import com.akiban.server.test.mt.mthapi.common.JsonUtils;
import com.akiban.server.service.session.Session;
import com.akiban.util.ThreadlessRandom;
import org.json.JSONException;
import org.json.JSONObject;
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
    private static final int CUSTOMERS_COUNT = 100;
    private static final int ORDERS_PER_CUSTOMER = 3;
    private static final int ITEMS_PER_ORDER = 3;
    private static final int ADDRESSES_PER_CUSTOMER = 2;

    @Test
    public void addDropTables() {
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

        SaisTable customer = builder.getSoleRootTable();

        WriteThread writeThread = getAddDropTableThread(customer);

        runThreads(writeThread,
                readPkCustomers(customer, .25f),
                readPkOrders(customer.getChild("orders"), .25f),
                readParentItems(customer.getChild("orders").getChild("items"), .25f),
                readPkAddresses(customer.getChild("addresses"), .25f)
        );
    }

    private abstract static class MyReadThread extends OptionallyWorkingReadThread {
        MyReadThread(SaisTable root, float chance) {
            super(SCHEMA, root, chance, false,
                    HapiRequestException.ReasonCode.UNKNOWN_IDENTIFIER,
                    HapiRequestException.ReasonCode.UNSUPPORTED_REQUEST);
        }

        @Override
        protected void validateErrorResponse(HapiGetRequest request, Throwable exception) throws UnexpectedException {
            if (!causeIsOldAIS(exception)) {
                super.validateErrorResponse(request, exception);
            }
        }

        protected static boolean causeIsOldAIS(Throwable exception) {
            if (exception instanceof HapiRequestException) {
                Throwable cause = exception.getCause();
                if (cause != null && (
                        cause.getClass().equals(OldAISException.class)
                        || cause.getClass().equals(TableDefinitionChangedException.class))
                ) {
                    return true; // expected
                }
            }
            return false;
        }
    }

    private static OptionallyWorkingReadThread readPkCustomers(final SaisTable customer, float chance) {
        return new MyReadThread(customer, chance) {
            @Override
            protected HapiRequestStruct pullRequest(ThreadlessRandom random) {
                int pk = random.nextInt(0, CUSTOMERS_COUNT);
                HapiGetRequest request =  DefaultHapiGetRequest.forTable(SCHEMA, customer.getName())
                        .withEqualities("cid", Integer.toString(pk))
                        .done();
                return new HapiRequestStruct(request, customer, "PRIMARY");
            }
        };
    }

    private static OptionallyWorkingReadThread readPkOrders(final SaisTable orders, float chance) {
        return new MyReadThread(orders, chance) {
            @Override
            protected HapiRequestStruct pullRequest(ThreadlessRandom random) {
                int cid = random.nextInt(0, CUSTOMERS_COUNT);
                int oid = random.nextInt(0, ORDERS_PER_CUSTOMER);
                HapiGetRequest request =  DefaultHapiGetRequest.forTable(SCHEMA, orders.getName())
                        .withEqualities("oid", Integer.toString(oid))
                        .and("c_id", Integer.toString(cid))
                        .done();
                return new HapiRequestStruct(request, orders, "PRIMARY");
            }
        };
    }

    private static OptionallyWorkingReadThread readParentItems(final SaisTable items, float chance) {
        return new MyReadThread(items, chance) {
            @Override
            protected HapiRequestStruct pullRequest(ThreadlessRandom random) {
                int cid = random.nextInt(0, CUSTOMERS_COUNT);
                int oid = random.nextInt(0, ORDERS_PER_CUSTOMER);
                HapiGetRequest request =  DefaultHapiGetRequest.forTable(SCHEMA, items.getName())
                        .withEqualities("o_id", Integer.toString(oid))
                        .and("c_id", Integer.toString(cid))
                        .done();
                return new HapiRequestStruct(request, items, null); // TODO this index is knowable
            }

            @Override
            protected void validateSuccessResponse(HapiRequestStruct requestStruct, JSONObject result) throws JSONException {
                JsonUtils.validateResponse(result, requestStruct.getSelectRoot(), requestStruct.getPredicatesTable());
            }
        };
    }

    private static OptionallyWorkingReadThread readPkAddresses(final SaisTable addresses, float chance) {
        return new MyReadThread(addresses, chance) {
            @Override
            protected HapiRequestStruct pullRequest(ThreadlessRandom random) {
                int cid = random.nextInt(0, CUSTOMERS_COUNT);
                int addressCount = random.nextInt(0, ADDRESSES_PER_CUSTOMER);
                int aid = aid(cid, addressCount);
                HapiGetRequest request =  DefaultHapiGetRequest.forTable(SCHEMA, addresses.getName())
                        .withEqualities("c_id", Integer.toString(cid))
                        .and("aid", Integer.toString(aid))
                        .done();
                return new HapiRequestStruct(request, addresses, null); // TODO this index is knowable
            }
        };
    }

    private WriteThread getAddDropTableThread(final SaisTable customer) {
        return new AddDropTablesWriter(customer) {

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
                        NewRow row = createNewRow(tableId, oid, cid, priority(cid, oid));
                        dml.writeRow(session, row);
                    }
                }
            }

            private void items(DMLFunctions dml, Session session, int tableId) throws InvalidOperationException {
                int iid = 0;
                for (int cid = 0; cid < CUSTOMERS_COUNT; ++cid) {
                    for (int oid = 0; oid < ORDERS_PER_CUSTOMER; ++oid) {
                        for (int count=0; count < ITEMS_PER_ORDER; ++count) {
                            NewRow row = createNewRow(tableId, iid, oid, cid, -1L);
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
        };
    }

    private static int age(int cid) {
        return cid * 2;
    }

    private static int priority(int oid, int cid) {
        return cid % 2 == 0 ? oid : -oid;
    }

    private static int aid(int cid, int count) {
        assert count >= 0 && count < 10 : count;
        return (cid * 10) + count;
    }

    private abstract static class AddDropTablesWriter implements WriteThread {

        /**
         * How many children each table has. If negative, the table doesn't exist.
         */
        private final Map<SaisTable,Integer> tablesMap;
        private final List<SaisTable> tablesList;
        private final ThreadlessRandom rand = new ThreadlessRandom();

        AddDropTablesWriter(SaisTable root) {
            tablesMap = new HashMap<SaisTable, Integer>();
            for (SaisTable table : root.setIncludingChildren()) {
                tablesMap.put(table, -1);
            }
            tablesList = new ArrayList<SaisTable>();
            tablesList.add(root);
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

            switch (tablesMap.get(table)) {
                case -1: // doesn't exist, so create it!
                    int tableId = addTable(session, ddl, table);
                    writeTableRows(session, dml, tableId, table);
                    LOG.trace("added table {} (with rows) id = {}", table, tableId);
                    break;
                case 0: // is a leaf, so drop it
                    LOG.trace("about to drop table {}", table);
                    dropTable(session, ddl, table);
                    LOG.trace("dropped table {}", table);
                    break;
                default:
                    throw new UnsupportedOperationException(String.format("%s: %d", table, tablesMap.get(table)));
            }
        }

        private int addTable(Session session, DDLFunctions ddl, SaisTable table) throws InvalidOperationException {
            String ddlText = DDLUtils.buildDDL(table);
            SchemaFactory schemaFactory = new SchemaFactory(SCHEMA);
            AkibanInformationSchema tempAIS = schemaFactory.ais(ddl.getAIS(session), ddlText);

            ddl.createTable(session, tempAIS.getUserTable(SCHEMA, table.getName()));
            int tableId = ddl.getTableId(session, new TableName(SCHEMA, table.getName()));

            tablesMap.put(table, 0);
            if (!tablesList.contains(table)) { // list is pre-seeded with root
                tablesList.add(table);
            }
            SaisFK parentFK = table.getParentFK();
            if (parentFK != null) {
                SaisTable parent = parentFK.getParent();
                tablesMap.put(parent, tablesMap.get(parent) + 1);
                tablesList.remove(parent);
            }
            for (SaisFK childFK : table.getChildren()) {
                SaisTable child = childFK.getChild();
                tablesMap.put(child, -1);
                tablesList.add(child);
            }

            return tableId;
        }

        protected abstract void writeTableRows(Session session, DMLFunctions dml, int tableId, SaisTable table)
                throws InvalidOperationException;

        private void dropTable(Session session, DDLFunctions ddl, SaisTable table) throws InvalidOperationException {
            ddl.dropTable(session, new TableName(SCHEMA, table.getName()));

            tablesMap.put(table, -1);
            assert tablesList.contains(table) : String.format("%s should have contained %s", tablesList, table);

            SaisFK parentFK = table.getParentFK();
            if (parentFK != null) {
                SaisTable parent = parentFK.getParent();
                int newChildCount = tablesMap.get(parent) - 1;
                tablesMap.put(parent, newChildCount);
                if (newChildCount == 0) {
                    tablesList.add(parent);
                }
            }
            for (SaisFK childFK : table.getChildren()) {
                SaisTable child = childFK.getChild();
                assert tablesMap.get(child) < 0 : String.format("%s: %s", child, tablesMap);
                tablesList.remove(child);
            }
        }

        @Override
        public boolean continueThroughException(Throwable throwable) {
            return false;
        }
    }
}
