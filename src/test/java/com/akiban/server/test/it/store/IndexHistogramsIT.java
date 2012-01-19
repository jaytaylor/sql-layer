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
import com.akiban.server.service.servicemanager.GuicedServiceManager;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.store.Store;
import com.akiban.server.store.statistics.IndexStatistics;
import com.akiban.server.store.statistics.IndexStatisticsService;
import com.akiban.server.store.statistics.IndexStatisticsServiceImpl;
import com.akiban.server.test.it.ITBase;
import com.akiban.util.ArgumentValidation;
import com.google.inject.Inject;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

public final class IndexHistogramsIT extends ITBase {

    @Test
    public void customersPk() {
        validateHistogram("customers", PK, 1, 32, CUSTOMERS_COUNT, CUSTOMERS_COUNT);
    }

    @Test
    public void customersNameDob_1() {
        validateHistogram("customers", "name_dob", 1, 32, CUSTOMERS_COUNT, CUSTOMERS_COUNT);
    }

    @Test
    public void customersNameDob_2() {
        validateHistogram("customers", "name_dob", 2, 32, CUSTOMERS_COUNT, CUSTOMERS_COUNT);
    }
    
    @Test
    public void ordersPk() {
        int count = ORDERS_COUNT * CUSTOMERS_COUNT;
        validateHistogram("orders", PK, 1, 32, count, count);
    }
    
    @Test
    public void ordersPlaced() {
        // "placed" has only 8 possible values, so all 8 should be there.
        Map<String,IndexStatistics.HistogramEntry> entries = validateHistogram("orders", "placed", 1, 8, 171, 8);
        // making a copy makes it easier to see the original set in a debugger
        Map<String,IndexStatistics.HistogramEntry> copy = new HashMap<String, IndexStatistics.HistogramEntry>(entries);
        for (int expectedPlaced = 0; expectedPlaced < 8; ++expectedPlaced) {
            IndexStatistics.HistogramEntry entry = removeEntry(copy, expectedPlaced);
            // the first three entries have 22 entries, the last 5 have 21
            int expectedEq = (expectedPlaced < 3) ? 22 : 21;
            assertEquals("equals count for " + entry, expectedEq, entry.getEqualCount());
            assertEquals("equals lt for " + entry, 0, entry.getLessCount());
            assertEquals("equals lt-distinct for " + entry, 0, entry.getDistinctCount());
        }
        assertTrue("entries left over: " + copy.keySet(), copy.isEmpty());
    }
    
    @Test
    public void itemsPk() {
        int count = ITEMS_COUNT * ORDERS_COUNT * CUSTOMERS_COUNT;
        validateHistogram("items", PK, 1, 32, count, count);
    }
    
    @Test
    public void skuByPlacedGI_1() {
        // there are only 9 skus, so only 9 buckets and distinct values. These 9 skus are uniformly distributed
        // among 684 items.
        Map<String,IndexStatistics.HistogramEntry> entries = validateHistogram(skuByPlaced, 1, 9, 684, 9);
        // making a copy makes it easier to see the original set in a debugger
        Map<String,IndexStatistics.HistogramEntry> copy = new HashMap<String, IndexStatistics.HistogramEntry>(entries);
        for (int expectedSku = 0; expectedSku < 9; ++expectedSku) {
            IndexStatistics.HistogramEntry entry = removeEntry(copy, expectedSku);
            assertEquals("equals count for " + entry, 76, entry.getEqualCount());
            assertEquals("equals lt for " + entry, 0, entry.getLessCount());
            assertEquals("equals lt-distinct for " + entry, 0, entry.getDistinctCount());
        }
        assertTrue("entries left over: " + copy.keySet(), copy.isEmpty());
    }

    @Test
    public void skuByPlacedGI_2() {
        // there are 684 items, each with a sku-placed entry.
        // there are 9 sku-values and 8 placed-values, meaning there are 9*8 = 72 distinct values.
        // This means there's very little we can really say about each entry, other than that its equals-count
        // is either 9 or 10.
        Map<String,IndexStatistics.HistogramEntry> entries = validateHistogram(skuByPlaced, 2, 32, 684, 72);

        // making a copy makes it easier to see the original set in a debugger
        Map<String,IndexStatistics.HistogramEntry> copy = new HashMap<String, IndexStatistics.HistogramEntry>(entries);
        for (int expectedSku = 0; expectedSku < 9; ++expectedSku) {
            for (int expectedPlaced = 0; expectedPlaced < 8; ++expectedPlaced) {
                String key = keyFor(expectedSku, expectedPlaced);
                IndexStatistics.HistogramEntry entry = copy.remove(key);
                if (entry != null) {
                    long equals = entry.getEqualCount();
                    assertTrue(
                            "equals count must be 9 or 10, was " + equals + ": " + entry,
                            equals == 9 || equals == 10);
                }
            }
        }
        assertTrue("entries left over: " + copy.keySet(), copy.isEmpty());
    }

    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
                .bind(IndexStatisticsService.class, NonTransactionallyInstallingStatsService.class);
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

        
        // data: customers
        int oid = 0;
        int iid = 0;
        for (int cid=0; cid < CUSTOMERS_COUNT; ++cid) {
            createCustomer(cTable, cid);
            for (int orderNum=0; orderNum < ORDERS_COUNT; ++orderNum) {
                createOrder(oTable, oid, cid);
                for (int itemNum=0; itemNum < ITEMS_COUNT; ++itemNum) {
                    createItem(iTable, iid++, oid);
                }
                ++oid; // only increment this after all items have used it!
            }
        }
    }

    private void createCustomer(int tableId, int cid) {
        final int dob;
        // values counts do not include 0, which I'm counting as a special case (more convenient accounting)
        if (cid == 0 || divisibleBy(cid, 19)) // 2 values: { 0, 19, 38 }
            dob = 2002;
        else if (divisibleBy(cid, 17))        // 7 values: { 7, 14, 21, 28, 35, 42, 49 }
            dob = 2007;
        else
            dob = 1999;
        assert cid >= 0 && cid < 1000 : cid;
        writeRow(tableId, s(cid), String.format("cust_#%03d", cid), s(dob));
    }

    private void createOrder(int tableId, int oid, int cid) {
        int offset = oid % 8;
        int base = 0;
        writeRow(tableId, s(oid), s(cid), s(offset + base));
    }

    private void createItem(int tableId, int iid, int oid) {
        writeRow(tableId, s(iid), s(oid), s(iid%9));
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

    private IndexStatistics.HistogramEntry removeEntry(Map<String,IndexStatistics.HistogramEntry> map, int... segments)
    {
        String key = keyFor(segments);
        IndexStatistics.HistogramEntry entry = map.remove(key);
        assertNotNull("no entry for key " + key, entry);
        return entry;
    }

    private String keyFor(int... segments) {
        ArgumentValidation.isGT("segments length", segments.length, 0);
        StringBuilder formatBuilder = new StringBuilder("{");
        Object[] args = new Object[segments.length];
        for (int i=0; i < segments.length; ++i) {
            formatBuilder.append("\"%04d\",");
            args[i] = segments[i];
        }
        formatBuilder.setLength(formatBuilder.length() - 1); // snip off the last comma
        String format = formatBuilder.append('}').toString();

        return String.format(format, args);
    }

    private Map<String,IndexStatistics.HistogramEntry> validateHistogram(
            String tableName,
            String indexName,
            int expectedColumns,
            int expectedEntries,
            int expectedCount,
            int expectedDistinct
    ) {
        Index index =  PK.endsWith(indexName)
                ? getUserTable(SCHEMA, tableName).getPrimaryKey().getIndex()
                : getUserTable(SCHEMA, tableName).getIndex(indexName);
        return validateHistogram(
                index, expectedColumns,
                expectedEntries,
                expectedCount,
                expectedDistinct
        );
    }

    private Map<String, IndexStatistics.HistogramEntry> validateHistogram(
            Index index, int expectedColumns, int expectedEntries, int expectedCount, int expectedDistinct
    ) {
        IndexStatisticsService statsService = statsService();
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
            actualDistinct += 1 + entry.getDistinctCount();

            result.put(entryString, entry);
            prev = entryString;
        }
        assertEquals("histogram rows count", expectedCount, actualCount);
        assertEquals("histogram distinct rows", expectedDistinct, actualDistinct);
        return result;
    }
    
    public static class NonTransactionallyInstallingStatsService extends IndexStatisticsServiceImpl {
        @Inject
        public NonTransactionallyInstallingStatsService(Store store, TreeService treeService,
                                                         SchemaManager schemaManager,
                                                         SessionService sessionService) {
            super(store, treeService, schemaManager, sessionService);
        }

        @Override
        protected void installUpdates(Session session, Map<? extends Index, ? extends IndexStatistics> updates) {
            updateCache(updates);
        }
    }

    private GroupIndex skuByPlaced;
    private Set<Index> analyzedIndexes = new HashSet<Index>();
    
    private static final String SCHEMA = "indexes";
    private static final String PK = "PK";
    private static final int CUSTOMERS_COUNT = 57;  //           57 customers
    private static final int ORDERS_COUNT = 3;      // 57 * 3  = 171 orders
    private static final int ITEMS_COUNT = 4;       // 171 * 4 = 684 items
}
