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
package com.akiban.server.test.it.store;

import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.Index;
import com.akiban.server.store.statistics.IndexStatistics;
import com.akiban.server.store.statistics.IndexStatisticsService;
import com.akiban.server.test.it.ITBase;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.fail;

public final class IndexHistogramsIT extends ITBase {

    @Test
    public void customersPk() {
        IndexStatistics stats = tIndex("customers", PK);
        IndexStatistics.Histogram one = stats.getHistogram(1);
        fail();
    }

    @Test
    public void customersNameDob() {
        IndexStatistics stats = tIndex("customers", "name_dob");
        IndexStatistics.Histogram one = stats.getHistogram(1);
        IndexStatistics.Histogram two = stats.getHistogram(2);
        fail();
    }
    
    @Test
    public void ordersPk() {
        IndexStatistics stats = tIndex("orders", PK);
        IndexStatistics.Histogram one = stats.getHistogram(1);
        fail();
    }
    
    @Test
    public void ordersPlaced() {
        IndexStatistics stats = tIndex("orders", "placed");
        IndexStatistics.Histogram one = stats.getHistogram(1);
        fail();
    }
    
    @Test
    public void itemsPk() {
        IndexStatistics stats = tIndex("items", PK);
        IndexStatistics.Histogram one = stats.getHistogram(1);
        fail();
    }
    
    @Test
    public void groupIndex() {
        IndexStatisticsService statsService = statsService();
        statsService.updateIndexStatistics(session(), Collections.singleton(skuByPlaced));
        IndexStatistics stats = statsService.getIndexStatistics(session(), skuByPlaced);
        IndexStatistics.Histogram one = stats.getHistogram(1);
        IndexStatistics.Histogram two = stats.getHistogram(2);
        fail();
    }
    
    private IndexStatistics tIndex(String table, String indexName) {
        Index index = PK.endsWith(indexName)
                ? getUserTable(SCHEMA, table).getPrimaryKey().getIndex()
                : getUserTable(SCHEMA, table).getIndex(indexName);

        IndexStatisticsService statsService = statsService();
        statsService.updateIndexStatistics(session(), Collections.singleton(index));
        return statsService.getIndexStatistics(session(), index);
    }

    @Before
    public void createDatabase() {
        // schema: tables
        int cTable = createTable(SCHEMA, "customers", "cid int key, name varchar(64), dob_year int",
                "key name_dob (name, dob_year)");
        int oTable = createTable(SCHEMA, "orders", "oid int key, cid int, placed varchar(64)",
                "key placed (placed)",
                akibanFK("cid", "customers", "cid"));
        int iTable = createTable(SCHEMA, "items", "iid int key, oid int, sku int",
                akibanFK("oid", "orders", "oid"));
        // schema: GIs
        String groupName = getUserTable(SCHEMA, "customers").getGroup().getName();
        skuByPlaced = createGroupIndex(groupName, "skuByPlaced", "items.sku,orders.placed");
        
        // data: customers
        int oid = 0;
        int iid = 0;
        for (int cid=0; cid < CUSTOMERS_COUNT; ++cid) {
            createCustomer(cTable, cid);
            for (int orderNum=0; orderNum < ORDERS_COUNT; ++orderNum) {
                createOrder(oTable, oid++, cid);
                for (int itemNum=0; itemNum < ITEMS_COUNT; ++itemNum) {
                    createItem(iTable, iid++, oid);
                }
            }
        }
    }

    private void createCustomer(int tableId, long cid) {
        final long dob;
        // values counts do not include 0, which I'm counting as a special case (more convenient accounting)
        if (cid == 0 || divisibleBy(cid, 53)) // 1 + 2 values: 53, 106
            dob = 33;
        else if (divisibleBy(cid, 17)) // 8 values
            dob = 88;
        else if (divisibleBy(cid, 7)) // 21 - 1 values 119 is preempted by %17
            dob = 2222;
        else
            dob = 11;
        assert cid >= 0 && cid < 1000 : cid;
        writeRow(tableId, cid, String.format("cust_#%03d", cid), dob);
    }

    private void createOrder(int tableId, long oid, long cid) {
        final long placed;
        if (divisibleBy(cid, 20) && divisibleBy(oid, 12))
            placed = 1234;
        else
            placed = 5678;
        writeRow(tableId, oid + cid, cid, placed);
    }

    private void createItem(int tableId, long iid, long oid) {
        writeRow(tableId, iid, oid, iid%3);
    }

    private boolean divisibleBy(long var, long denominator) {
        return var % denominator == 0;
    }

    private IndexStatisticsService statsService() {
        return serviceManager().getServiceByClass(IndexStatisticsService.class);
    }

    private GroupIndex skuByPlaced;
    
    private static final String SCHEMA = "indexes";
    private static final String PK = "PK";
    private static final int CUSTOMERS_COUNT = 57;// should be 150
    private static final int ORDERS_COUNT = 30;
    private static final int ITEMS_COUNT = 4;
}
