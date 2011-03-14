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

package com.akiban.server.itests.hapi.randomdb;

import com.akiban.server.InvalidOperationException;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.api.HapiGetRequest;
import com.akiban.server.api.HapiPredicate;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.itests.ApiTestBase;
import com.akiban.server.service.memcache.outputter.jsonoutputter.JsonOutputter;
import com.akiban.server.service.session.Session;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

// To run this test manually, set -DdebugMode=true. This will continue past test failures, print database contents,
// print each query, and for expected != actual, print both, nicely formatted. To investigate a single

public class RCTortureIT extends ApiTestBase
{
    @Test
    public void testRepeatedly() throws Exception
    {
        for (int t = 0; t < TRIALS; t++) {
            test(t);
            dropAllTables();
        }
    }

    Session testSession()
    {
        return session();
    }

    String table(int type)
    {
        return tableName(type).getTableName();
    }

    HKey hKey(NewRow row)
    {
        return new HKey(this, row);
    }

    void sort(List<NewRow> rows)
    {
        Collections.sort(rows, ROW_COMPARATOR);
    }

    DDLFunctions ddlFunctions()
    {
        return ddl();
    }

    int table(String schema, String table, String... definitions) throws InvalidOperationException
    {
        return super.createTable(schema, table, definitions);
    }

    void addRow(NewRow row) throws Exception
    {
        dml().writeRow(session(), row);
        db.add(row);
    }

    void printDB()
    {
        if (DEBUG) {
            print("----------------------------------------------------");
            for (NewRow row : db) {
                int tableId = row.getTableId();
                if (tableId == customerTable) {
                    print("customer  %s", row.get(0));
                } else if (tableId == orderTable) {
                    print("order     %s %s", row.get(0), row.get(1));
                } else if (tableId == itemTable) {
                    print("item      %s %s %s", row.get(0), row.get(1), row.get(2));
                } else if (tableId == addressTable) {
                    print("address   %s %s", row.get(0), row.get(1));
                }
            }
            print("----------------------------------------------------");
        }
    }

    void print(String template, Object... args)
    {
        if (DEBUG) {
            System.out.println(String.format(template, args));
        }
    }

    private void test(int trial) throws Exception
    {
        random = new Random(seed(trial));
        database.createSchema();
        database.populate();
        runQueries();
    }

    private void runQueries() throws Exception
    {
        if (false) {
/* Bug in KeyFilter. Peter is working on it.
            runQuery(orderTable, orderTable, Column.O_CID,  HapiPredicate.Operator.LT, 2);
   Looks like the same bug:
            runQuery(itemTable, itemTable, Column.I_CID,  HapiPredicate.Operator.LT, 2);
            runQuery(customerTable, orderTable, Column.O_CID,  HapiPredicate.Operator.LT, 2);
   Problem setting key segment differsAt:
*/
            runQuery(customerTable, itemTable, Column.I_CID,  HapiPredicate.Operator.EQ, 1);
            // runQuery(customerTable, addressTable, Column.A_CID,  HapiPredicate.Operator.EQ, 2);
        } else {
            for (HapiPredicate.Operator comparison : HapiPredicate.Operator.values()) {
                if (comparison != HapiPredicate.Operator.NE) {
                    for (int cid = 0; cid < MAX_CUSTOMERS; cid++) {
                        // One-table queries, e.g. schema:customer:cid>4
                        runQuery(customerTable, customerTable, Column.C_CID, comparison, cid);
                        runQuery(customerTable, customerTable, Column.C_CID_COPY, comparison, cid);
                        runQuery(orderTable, orderTable, Column.O_CID, comparison, cid);
                        runQuery(orderTable, orderTable, Column.O_CID_COPY, comparison, cid);
                        runQuery(itemTable, itemTable, Column.I_CID, comparison, cid);
                        runQuery(itemTable, itemTable, Column.I_CID_COPY, comparison, cid);
                        runQuery(addressTable, addressTable, Column.A_CID, comparison, cid);
                        runQuery(addressTable, addressTable, Column.A_CID_COPY, comparison, cid);
                        // Two-table queries, e.g. schema:customer:(order)oid=3
                        runQuery(customerTable, orderTable, Column.O_CID, comparison, cid);
                        runQuery(customerTable, orderTable, Column.O_CID_COPY, comparison, cid);
                        runQuery(customerTable, itemTable, Column.I_CID, comparison, cid);
                        runQuery(customerTable, itemTable, Column.I_CID_COPY, comparison, cid);
                        runQuery(orderTable, itemTable, Column.I_CID, comparison, cid);
                        runQuery(orderTable, itemTable, Column.I_CID_COPY, comparison, cid);
                        runQuery(customerTable, addressTable, Column.A_CID, comparison, cid);
                        runQuery(customerTable, addressTable, Column.A_CID_COPY, comparison, cid);
                    }
                }
            }
            // TODO: Query for oid, iid.
        }
    }

    private void runQuery(int rootTable,
                          int predicateTable,
                          Column predicateColumn,
                          HapiPredicate.Operator comparison,
                          int literal) throws Exception
    {
        String actualQueryResult =
            actual.queryResult(rootTable, predicateTable, predicateColumn, comparison, literal);
        String expectedQueryResult =
            expected.queryResult(rootTable, predicateTable, predicateColumn, comparison, literal);
        if (DEBUG) {
            if (!actualQueryResult.equals(expectedQueryResult)) {
                print("expected:\n%s", formatJSON(expectedQueryResult));
                print("actual:\n%s", formatJSON(actualQueryResult));
            }
        } else {
            assertEquals(expectedQueryResult, actualQueryResult);
        }
    }

    private String formatJSON(String json) throws JSONException
    {
        return new JSONObject(json).toString(4);
    }

    private static int seed(int trial)
    {
        return SEED + trial * 9987001;
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
    static final int TRIALS = 1;
    static final int SEED = 123456789;
    static final int MAX_CUSTOMERS = 3;
    static final int MAX_ORDERS_PER_CUSTOMER = 3;
    static final int MAX_ITEMS_PER_ORDER = 2;
    static final int MAX_ADDRESSES_PER_CUSTOMER = 2;
    static final boolean DEBUG = Boolean.getBoolean("debugMode");

    // Constants
    static final String SCHEMA = "schema";
    static final String COLON = ":";
    static final String OPEN = "(";
    static final String CLOSE = ")";

    // Test state
    Random random;
    int customerTable;
    int orderTable;
    int itemTable;
    int addressTable;
    List<NewRow> db = new ArrayList<NewRow>();
    final JsonOutputter outputter = new JsonOutputter();
    String query;
    HapiGetRequest request;
    private Expected expected = new Expected(this);
    private Actual actual = new Actual(this);
    private Database database = new Database(this);

}
