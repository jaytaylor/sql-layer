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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public final class IndexHistogramsIT extends ITBase {
    
    @Test
    public void customersPk() {
        validateHistogram("customers", PK, 1, 32, CUSTOMERS_COUNT, CUSTOMERS_COUNT);
    }

    @Test
    public void customersNameDob() {
        IndexStatistics stats = tIndexStats("customers", "name_dob");
        IndexStatistics.Histogram one = stats.getHistogram(1);
        IndexStatistics.Histogram two = stats.getHistogram(2);
        fail();
    }
    
    @Test
    public void ordersPk() {
        IndexStatistics stats = tIndexStats("orders", PK);
        IndexStatistics.Histogram one = stats.getHistogram(1);
        fail();
    }
    
    @Test
    public void ordersPlaced() {
        IndexStatistics stats = tIndexStats("orders", "placed");
        IndexStatistics.Histogram one = stats.getHistogram(1);
        fail();
    }
    
    @Test
    public void itemsPk() {
        IndexStatistics stats = tIndexStats("items", PK);
        IndexStatistics.Histogram one = stats.getHistogram(1);
        fail();
    }
    
    @Test
    public void skuByPlacedGI() {
        IndexStatisticsService statsService = statsService();
        statsService.updateIndexStatistics(session(), Collections.singleton(skuByPlaced));
        IndexStatistics stats = statsService.getIndexStatistics(session(), skuByPlaced);
        IndexStatistics.Histogram one = stats.getHistogram(1);
        IndexStatistics.Histogram two = stats.getHistogram(2);
        fail();
    }

    @Test
    public void cidOidGI() {
        IndexStatisticsService statsService = statsService();
        statsService.updateIndexStatistics(session(), Collections.singleton(cidOid));
        IndexStatistics stats = statsService.getIndexStatistics(session(), cidOid);
        IndexStatistics.Histogram one = stats.getHistogram(1);
        IndexStatistics.Histogram two = stats.getHistogram(2);
        fail();
    }
    
    private IndexStatistics tIndexStats(String table, String indexName) {
        Index index = tIndex(table, indexName);

        IndexStatisticsService statsService = statsService();
        statsService.updateIndexStatistics(session(), Collections.singleton(index));
        return statsService.getIndexStatistics(session(), index);
    }

    private Index tIndex(String table, String indexName) {
        return PK.endsWith(indexName)
                    ? getUserTable(SCHEMA, table).getPrimaryKey().getIndex()
                    : getUserTable(SCHEMA, table).getIndex(indexName);
    }

    @Before
    public void createDatabase() {
        // schema: tables
        int cTable = createTable(SCHEMA, "customers", "cid varchar(4) key, name varchar(64), dob_year varchar(4)",
                "key name_dob (name, dob_year)");
        int oTable = createTable(SCHEMA, "orders", "oid varchar(4) key, cid varchar(4), placed varchar(64)",
                "key placed (placed)",
                akibanFK("cid", "customers", "cid"));
        int iTable = createTable(SCHEMA, "items", "iid varchar(4) key, oid varchar(4), sku varchar(4)",
                akibanFK("oid", "orders", "oid"));
        // schema: GIs
        String groupName = getUserTable(SCHEMA, "customers").getGroup().getName();
        skuByPlaced = createGroupIndex(groupName, "skuByPlaced", "items.sku,orders.placed");
        cidOid = createGroupIndex(groupName, "cidOid", "customers.cid,orders.oid");
        
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

    private void createCustomer(int tableId, int cid) {
        final int dob;
        // values counts do not include 0, which I'm counting as a special case (more convenient accounting)
        if (cid == 0 || divisibleBy(cid, 53)) // 1 + 2 values: 53, 106
            dob = 1933;
        else if (divisibleBy(cid, 17)) // 8 values
            dob = 1988;
        else if (divisibleBy(cid, 7)) // 21 - 1 values 119 is preempted by %17
            dob = 2022;
        else
            dob = 1911;
        assert cid >= 0 && cid < 1000 : cid;
        writeRow(tableId, s(cid), String.format("cust_#%03d", cid), s(dob));
    }

    private void createOrder(int tableId, int oid, int cid) {
        final int placed;
        if (divisibleBy(cid, 20) && divisibleBy(oid, 12))
            placed = 1234;
        else
            placed = 5678;
        writeRow(tableId, s(oid + cid), s(cid), s(placed));
    }

    private void createItem(int tableId, int iid, int oid) {
        writeRow(tableId, s(iid), s(oid), s(iid%3));
    }

    private boolean divisibleBy(long var, long denominator) {
        return var % denominator == 0;
    }

    private IndexStatisticsService statsService() {
        return serviceManager().getServiceByClass(IndexStatisticsService.class);
    }
    
    private String s(int intValue) {
        assert intValue >= 0 && intValue < 10000 : intValue;
        return String.format("%04d", intValue);
    }

    private Map<String,IndexStatistics.HistogramEntry> validateHistogram(
            String tableName,
            String indexName,
            int expectedColumns,
            int expectedEntries,
            int expectedCount,
            int expectedDistinct
    ) {
        IndexStatisticsService statsService = statsService();
        Index index = tIndex(tableName, indexName);
        if (analyzedIndexes.add(index))
            statsService.updateIndexStatistics(session(), Collections.singleton(index));
        IndexStatistics stats = statsService.getIndexStatistics(session(), index);
        IndexStatistics.Histogram histogram = stats.getHistogram(expectedColumns);


        assertEquals("histogram index", index, histogram.getIndex());
        assertEquals("histogram column count", expectedColumns, histogram.getColumnCount());
        assertEquals("histogram entries count", expectedEntries, histogram.getEntries().size());

        Map<String,IndexStatistics.HistogramEntry> result = new HashMap<String, IndexStatistics.HistogramEntry>();
        String prev = null;
        int actualCount = 0;
        int actualDistinct = 0;
        for (IndexStatistics.HistogramEntry entry : histogram.getEntries()) {
            String entryString = entry.getKeyString();
            assertNotNull("entry key string should not be null: " + entry, entryString);
            if (prev != null && entryString.compareTo(prev) <= 0)
                fail(String.format("<%s> not greater than <%s>", entryString, prev));

            actualCount += entry.getEqualCount() + entry.getLessCount();
            actualDistinct += entry.getEqualCount() + entry.getDistinctCount();

            result.put(entryString, entry);
            prev = entryString;
        }
        assertEquals("histogram rows count", expectedCount, actualCount);
        assertEquals("histogram distinct rows", expectedDistinct, actualDistinct);
        return result;
    }

    private GroupIndex skuByPlaced;
    private GroupIndex cidOid;
    private Set<Index> analyzedIndexes = new HashSet<Index>();

    
    private static final String SCHEMA = "indexes";
    private static final String PK = "PK";
    private static final int CUSTOMERS_COUNT = 57;// should be 150
    private static final int ORDERS_COUNT = 30;
    private static final int ITEMS_COUNT = 4;
}
