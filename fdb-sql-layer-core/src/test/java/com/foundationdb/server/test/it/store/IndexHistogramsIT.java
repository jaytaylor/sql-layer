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

package com.foundationdb.server.test.it.store;

import com.foundationdb.ais.model.GroupIndex;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.service.transaction.TransactionService.CloseableTransaction;
import com.foundationdb.server.store.statistics.*;
import com.foundationdb.server.store.statistics.histograms.Sampler;
import com.foundationdb.server.test.it.ITBase;
import com.foundationdb.util.AssertUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public final class IndexHistogramsIT extends ITBase {
    
    @Test
    public void customersPk() {
        validateHistogram("customers", PK, 1,
                          entry("{\"0000\"}", 1, 0, 0),
                          entry("{\"0010\"}", 1, 9, 9),
                          entry("{\"0020\"}", 1, 9, 9),
                          entry("{\"0030\"}", 1, 9, 9),
                          entry("{\"0041\"}", 1, 10, 10),
                          entry("{\"0051\"}", 1, 9, 9),
                          entry("{\"0061\"}", 1, 9, 9),
                          entry("{\"0072\"}", 1, 10, 10),
                          entry("{\"0082\"}", 1, 9, 9),
                          entry("{\"0092\"}", 1, 9, 9),
                          entry("{\"0103\"}", 1, 10, 10),
                          entry("{\"0113\"}", 1, 9, 9),
                          entry("{\"0123\"}", 1, 9, 9),
                          entry("{\"0134\"}", 1, 10, 10),
                          entry("{\"0144\"}", 1, 9, 9),
                          entry("{\"0154\"}", 1, 9, 9),
                          entry("{\"0165\"}", 1, 10, 10),
                          entry("{\"0175\"}", 1, 9, 9),
                          entry("{\"0185\"}", 1, 9, 9),
                          entry("{\"0196\"}", 1, 10, 10),
                          entry("{\"0206\"}", 1, 9, 9),
                          entry("{\"0216\"}", 1, 9, 9),
                          entry("{\"0227\"}", 1, 10, 10),
                          entry("{\"0237\"}", 1, 9, 9),
                          entry("{\"0247\"}", 1, 9, 9),
                          entry("{\"0258\"}", 1, 10, 10),
                          entry("{\"0268\"}", 1, 9, 9),
                          entry("{\"0278\"}", 1, 9, 9),
                          entry("{\"0289\"}", 1, 10, 10),
                          entry("{\"0299\"}", 1, 9, 9),
                          entry("{\"0309\"}", 1, 9, 9),
                          entry("{\"0319\"}", 1, 9, 9));
    }

    @Test
    public void customersNameDob_1() {
        // 320 customers with 4 names, evenly distributed, is 80 customers per name
        validateHistogram("customers", "name_dob", 1,
                entry("{null}", 80, 0, 0),
                entry("{\"Bob\"}", 80, 0, 0),
                entry("{\"Carla\"}", 80, 0, 0),
                entry("{\"Dot\"}", 80, 0, 0)
        );
    }

    @Test
    public void customersNameDob_2() {
        // 320 customers with 4 names, evenly distributed, is 80 customers per name
        // dob is 1983 if cid is divisible by 4, otherwise  it's 1900 + (cid / 2).
        // Since both patterns have 4, that means all 80 null-names will have 1983 (they're all with cids divisible by
        // 4), and the rest will be striped evenly. Each bucket will represent 7 or 8 entries.
        // This algorithm is a bit tricky to pin down, so I'm putting the results in manually
        HistogramEntryDescription[] expected = new HistogramEntryDescription[32];
        int i = 0;
        expected[i++] = entry("{null,\"1983\"}", 80, 0, 0);
        expected[i++] = entry("{\"Bob\",\"1914\"}", 1, 7, 7);
        expected[i++] = entry("{\"Bob\",\"1930\"}", 1, 7, 7);
        expected[i++] = entry("{\"Bob\",\"1946\"}", 1, 7, 7);
        expected[i++] = entry("{\"Bob\",\"1960\"}", 1, 6, 6);
        expected[i++] = entry("{\"Bob\",\"1976\"}", 1, 7, 7);
        expected[i++] = entry("{\"Bob\",\"1992\"}", 1, 7, 7);
        expected[i++] = entry("{\"Bob\",\"2008\"}", 1, 7, 7);
        expected[i++] = entry("{\"Bob\",\"2022\"}", 1, 6, 6);
        expected[i++] = entry("{\"Bob\",\"2038\"}", 1, 7, 7);
        expected[i++] = entry("{\"Bob\",\"2054\"}", 1, 7, 7);
        expected[i++] = entry("{\"Carla\",\"1911\"}", 1, 7, 7);
        expected[i++] = entry("{\"Carla\",\"1925\"}", 1, 6, 6);
        expected[i++] = entry("{\"Carla\",\"1941\"}", 1, 7, 7);
        expected[i++] = entry("{\"Carla\",\"1957\"}", 1, 7, 7);
        expected[i++] = entry("{\"Carla\",\"1973\"}", 1, 7, 7);
        expected[i++] = entry("{\"Carla\",\"1987\"}", 1, 6, 6);
        expected[i++] = entry("{\"Carla\",\"2003\"}", 1, 7, 7);
        expected[i++] = entry("{\"Carla\",\"2019\"}", 1, 7, 7);
        expected[i++] = entry("{\"Carla\",\"2035\"}", 1, 7, 7);
        expected[i++] = entry("{\"Carla\",\"2049\"}", 1, 6, 6);
        expected[i++] = entry("{\"Dot\",\"1905\"}", 1, 7, 7);
        expected[i++] = entry("{\"Dot\",\"1921\"}", 1, 7, 7);
        expected[i++] = entry("{\"Dot\",\"1937\"}", 1, 7, 7);
        expected[i++] = entry("{\"Dot\",\"1951\"}", 1, 6, 6);
        expected[i++] = entry("{\"Dot\",\"1967\"}", 1, 7, 7);
        expected[i++] = entry("{\"Dot\",\"1983\"}", 1, 7, 7);
        expected[i++] = entry("{\"Dot\",\"1999\"}", 1, 7, 7);
        expected[i++] = entry("{\"Dot\",\"2013\"}", 1, 6, 6);
        expected[i++] = entry("{\"Dot\",\"2029\"}", 1, 7, 7);
        expected[i++] = entry("{\"Dot\",\"2045\"}", 1, 7, 7);
        expected[i++] = entry("{\"Dot\",\"2059\"}", 1, 6, 6);
        validateHistogram("customers", "name_dob", 2, expected);
    }
    
    @Test
    public void ordersPk() {
        validateHistogram("orders", PK, 1, 
                          entry("{(long)0}", 1, 0, 0),
                          entry("{(long)30}", 1, 29, 29),
                          entry("{(long)61}", 1, 30, 30),
                          entry("{(long)92}", 1, 30, 30),
                          entry("{(long)123}", 1, 30, 30),
                          entry("{(long)154}", 1, 30, 30),
                          entry("{(long)185}", 1, 30, 30),
                          entry("{(long)216}", 1, 30, 30),
                          entry("{(long)247}", 1, 30, 30),
                          entry("{(long)278}", 1, 30, 30),
                          entry("{(long)309}", 1, 30, 30),
                          entry("{(long)340}", 1, 30, 30),
                          entry("{(long)371}", 1, 30, 30),
                          entry("{(long)402}", 1, 30, 30),
                          entry("{(long)433}", 1, 30, 30),
                          entry("{(long)464}", 1, 30, 30),
                          entry("{(long)495}", 1, 30, 30),
                          entry("{(long)526}", 1, 30, 30),
                          entry("{(long)557}", 1, 30, 30),
                          entry("{(long)588}", 1, 30, 30),
                          entry("{(long)619}", 1, 30, 30),
                          entry("{(long)650}", 1, 30, 30),
                          entry("{(long)681}", 1, 30, 30),
                          entry("{(long)712}", 1, 30, 30),
                          entry("{(long)743}", 1, 30, 30),
                          entry("{(long)774}", 1, 30, 30),
                          entry("{(long)805}", 1, 30, 30),
                          entry("{(long)836}", 1, 30, 30),
                          entry("{(long)867}", 1, 30, 30),
                          entry("{(long)898}", 1, 30, 30),
                          entry("{(long)929}", 1, 30, 30),
                          entry("{(long)959}", 1, 29, 29));
    }
    
    @Test
    public void ordersPlaced() {
        List<HistogramEntryDescription> expected = new ArrayList<>();

        // div-by-5s first
        expected.add(entry("{\"0000\"}", 120, 0, 0));
        // build up every 20 entries, plus an entry for the end
        int lessThans = 0;
        int lessThansOffset = 0;
        int max = CUSTOMERS_COUNT * ORDERS_COUNT;
        for(int oid=0; oid < max; ++oid) {
            if (oid == 5) {
                expected.add(entry("{\"0005\"}", 121, lessThans, lessThans));
                lessThansOffset = lessThans;
                continue;
            }
            if (divisibleBy(oid, 4))
                continue;
            boolean hasEntry = (oid  == 959) || lessThans == 23; // last value is 559, but it's divisible by 7
            if (hasEntry) {
                int lessCount = lessThans - lessThansOffset;
                expected.add(entry(String.format("{\"%04d\"}", oid), 1, lessCount, lessCount));
                lessThansOffset = 0;
                lessThans = 0;
            }
            else {
                ++lessThans;
            }
        }
        validateHistogram("orders", "placed", 1, expected.toArray(new HistogramEntryDescription[expected.size()]));
    }

    @Test
    public void namePlacedGI_1() {
        // 320*3=960 customers with 4 names, evenly distributed, is 240 customers per name
        validateHistogram(namePlacedGi, 1,
                entry("{null}", 240, 0, 0),
                entry("{\"Bob\"}", 240, 0, 0),
                entry("{\"Carla\"}", 240, 0, 0),
                entry("{\"Dot\"}", 240, 0, 0)
        );
    }

    @Test
    public void namePlacedGI_2() {
        // I ran the tested, eyeballed the results, and copied them here.
        validateHistogram(namePlacedGi, 2,
                entry("{null,\"0000\"}", 40, 0, 0),
                entry("{null,\"0005\"}", 40, 2, 2),
                entry("{null,\"0158\"}", 1, 25, 25),
                entry("{null,\"0326\"}", 1, 27, 27),
                entry("{null,\"0493\"}", 1, 26, 26),
                entry("{null,\"0661\"}", 1, 27, 27),
                entry("{null,\"0829\"}", 1, 27, 27),
                entry("{\"Bob\",\"0000\"}", 40, 21, 21),
                entry("{\"Bob\",\"0005\"}", 41, 1, 1),
                entry("{\"Bob\",\"0039\"}", 1, 4, 4),
                entry("{\"Bob\",\"0207\"}", 1, 27, 27),
                entry("{\"Bob\",\"0375\"}", 1, 27, 27),
                entry("{\"Bob\",\"0533\"}", 1, 26, 26),
                entry("{\"Bob\",\"0701\"}", 1, 27, 27),
                entry("{\"Bob\",\"0869\"}", 1, 27, 27),
                entry("{\"Carla\",\"0000\"}", 40, 14, 14),
                entry("{\"Carla\",\"0005\"}", 40, 0, 0),
                entry("{\"Carla\",\"0078\"}", 1, 12, 12),
                entry("{\"Carla\",\"0246\"}", 1, 27, 27),
                entry("{\"Carla\",\"0414\"}", 1, 27, 27),
                entry("{\"Carla\",\"0571\"}", 1, 26, 26),
                entry("{\"Carla\",\"0739\"}", 1, 27, 27),
                entry("{\"Carla\",\"0907\"}", 1, 27, 27),
                entry("{\"Dot\",\"0081\"}", 1, 26, 26),
                entry("{\"Dot\",\"0190\"}", 1, 27, 27),
                entry("{\"Dot\",\"0299\"}", 1, 27, 27),
                entry("{\"Dot\",\"0407\"}", 1, 26, 26),
                entry("{\"Dot\",\"0525\"}", 1, 27, 27),
                entry("{\"Dot\",\"0634\"}", 1, 27, 27),
                entry("{\"Dot\",\"0742\"}", 1, 26, 26),
                entry("{\"Dot\",\"0851\"}", 1, 27, 27),
                entry("{\"Dot\",\"0959\"}", 1, 26, 26)
        );
    }

    @Test
    public void edgeAnalysis() {
        int cTable = getTable(SCHEMA, "customers").getTableId();
        int oTable = getTable(SCHEMA, "orders").getTableId();
        int maxCid = bucketCount() * Sampler.OVERSAMPLE_FACTOR;
        insertRows(cTable, oTable, CUSTOMERS_COUNT, maxCid);
        validateHistogram("customers", PK, 1, 
                          entry("{\"0000\"}", 1, 0, 0),
                          entry("{\"0051\"}", 1, 50, 50),
                          entry("{\"0103\"}", 1, 51, 51),
                          entry("{\"0154\"}", 1, 50, 50),
                          entry("{\"0206\"}", 1, 51, 51),
                          entry("{\"0258\"}", 1, 51, 51),
                          entry("{\"0309\"}", 1, 50, 50),
                          entry("{\"0361\"}", 1, 51, 51),
                          entry("{\"0412\"}", 1, 50, 50),
                          entry("{\"0464\"}", 1, 51, 51),
                          entry("{\"0516\"}", 1, 51, 51),
                          entry("{\"0567\"}", 1, 50, 50),
                          entry("{\"0619\"}", 1, 51, 51),
                          entry("{\"0670\"}", 1, 50, 50),
                          entry("{\"0722\"}", 1, 51, 51),
                          entry("{\"0774\"}", 1, 51, 51),
                          entry("{\"0825\"}", 1, 50, 50),
                          entry("{\"0877\"}", 1, 51, 51),
                          entry("{\"0929\"}", 1, 51, 51),
                          entry("{\"0980\"}", 1, 50, 50),
                          entry("{\"1032\"}", 1, 51, 51),
                          entry("{\"1083\"}", 1, 50, 50),
                          entry("{\"1135\"}", 1, 51, 51),
                          entry("{\"1187\"}", 1, 51, 51),
                          entry("{\"1238\"}", 1, 50, 50),
                          entry("{\"1290\"}", 1, 51, 51),
                          entry("{\"1341\"}", 1, 50, 50),
                          entry("{\"1393\"}", 1, 51, 51),
                          entry("{\"1445\"}", 1, 51, 51),
                          entry("{\"1496\"}", 1, 50, 50),
                          entry("{\"1548\"}", 1, 51, 51),
                          entry("{\"1599\"}", 1, 50, 50));
    }

    @Test
    public void largeAnalysis() {
        int cTable = getTable(SCHEMA, "customers").getTableId();
        int oTable = getTable(SCHEMA, "orders").getTableId();
        int maxCid = bucketCount() * Sampler.OVERSAMPLE_FACTOR+1;
        insertRows(cTable, oTable, CUSTOMERS_COUNT, maxCid);
        validateHistogram("customers", PK, 1,
                          entry("{\"0000\"}", 1, 0, 0),
                          entry("{\"0051\"}", 1, 50,50),
                          entry("{\"0103\"}", 1, 51,51),
                          entry("{\"0154\"}", 1, 50,50),
                          entry("{\"0206\"}", 1, 51,51),
                          entry("{\"0258\"}", 1, 51,51),
                          entry("{\"0309\"}", 1, 50,50),
                          entry("{\"0361\"}", 1, 51,51),
                          entry("{\"0413\"}", 1, 51,51),
                          entry("{\"0464\"}", 1, 50,50),
                          entry("{\"0516\"}", 1, 51,51),
                          entry("{\"0568\"}", 1, 51,51),
                          entry("{\"0619\"}", 1, 50,50),
                          entry("{\"0671\"}", 1, 51,51),
                          entry("{\"0723\"}", 1, 51,51),
                          entry("{\"0774\"}", 1, 50,50),
                          entry("{\"0826\"}", 1, 51,51),
                          entry("{\"0877\"}", 1, 50,50),
                          entry("{\"0929\"}", 1, 51,51),
                          entry("{\"0981\"}", 1, 51,51),
                          entry("{\"1032\"}", 1, 50,50),
                          entry("{\"1084\"}", 1, 51,51),
                          entry("{\"1136\"}", 1, 51,51),
                          entry("{\"1187\"}", 1, 50,50),
                          entry("{\"1239\"}", 1, 51,51),
                          entry("{\"1291\"}", 1, 51,51),
                          entry("{\"1342\"}", 1, 50,50),
                          entry("{\"1394\"}", 1, 51,51),
                          entry("{\"1446\"}", 1, 51,51),
                          entry("{\"1497\"}", 1, 50,50),
                          entry("{\"1549\"}", 1, 51,51),
                          entry("{\"1600\"}", 1, 50,50));
    }

    @Test
    public void oversampleNotEvenlyDistributed() {
        int cTable = getTable(SCHEMA, "customers").getTableId();
        int oTable = getTable(SCHEMA, "orders").getTableId();
        double interval = 2.02;
        double oversamples = bucketCount() * Sampler.OVERSAMPLE_FACTOR;
        int maxCid = (int) Math.round(interval * oversamples);
        insertRows(cTable, oTable, CUSTOMERS_COUNT, maxCid);

        validateHistogram("customers", PK, 1, (HistogramEntryDescription[])null);
    }

    /**
     * <p>Initializes and populates the database.</p>
     *
     * <p>Customers are created as:
     * <ul>
     *     <li><b>cid</b> is monotonically increasing</li>
     *     <li><b>name</b> is one of null, "Bob", "Carla" or "Dot", rotated by cid</li>
     *     <li><b>dob_year</b> is 1983 if cid is divisible by 4, otherwise  it's 1900 + (cid / 2)</li>
     * </ul>
     * </p>
     *
     * <p>There are three orders per customer, and they are created as:
     * <ul>
     *     <li><b>oid</b> is monotonically increasing</li>
     *     <li><b>cid</b> corresponds to the parent cid</li>
     *     <li><b>placed</b> is
     *     <ul>
     *         <li>0 if oid is divisible by 5</li>
     *         <li>50 if oid is divisible by 7 (and not by 5)</li>
     *         <li>oid otherwise</li>
     *     </ul>
     *     </li>
     * </ul>
     * </p>
     */
    @Before
    public void createDatabase() {
        // schema: tables
        int cTable = createTable(SCHEMA, "customers", "cid varchar(4) not null primary key, name varchar(64), dob_year varchar(4)");
        createIndex(SCHEMA, "customers", "name_dob", "name", "dob_year");
        int oTable = createTable(SCHEMA, "orders", "oid int not null primary key, cid varchar(4), placed varchar(64)",
                akibanFK("cid", "customers", "cid"));
        createIndex(SCHEMA, "orders", "placed", "placed");
        // schema: GIs
        TableName groupName = getTable(SCHEMA, "customers").getGroup().getName();
        namePlacedGi = createLeftGroupIndex(groupName, "namePlaced", "customers.name", "orders.placed");

        // insert data
        int startingCid = 0;
        int endingCid = CUSTOMERS_COUNT;
        insertRows(cTable, oTable, startingCid, endingCid);
    }

    private void insertRows(int cTable, int oTable, int startingCid, int endingCid) {
        try(CloseableTransaction txn = txnService().beginCloseableTransaction(session())) {
            insertRowsInternal(cTable, oTable, startingCid, endingCid);
            txn.commit();
        }
    }
    private void insertRowsInternal(int cTable, int oTable, int startingCid, int endingCid) {
        String[] names = {null, "Bob", "Carla", "Dot"};
        for (int cid=startingCid; cid < endingCid; ++cid) {
            // customer
            String name = names[ cid % names.length ];
            int dobYear = (divisibleBy(cid, 4)) ? 1983 : (1900 + cid/2);
            writeRow(cTable, s(cid), name, s(dobYear));

            // customer's orders
            for (int orderNum=0; orderNum < ORDERS_COUNT; ++orderNum) {
                long oid = oidCounter++;
                long placed;
                if (divisibleBy(oid, 8))
                    placed = 0;
                else if (divisibleBy(oid, 4))
                    placed = 5;
                else
                    placed = oid;
                writeRow(oTable, oid, s(cid), s(placed));
            }

            txnService().periodicallyCommit(session());
        }
    }

    private boolean divisibleBy(long var, long denominator) {
        return var % denominator == 0;
    }

    private IndexStatisticsService statsService() {
        return serviceManager().getServiceByClass(IndexStatisticsService.class);
    }
    
    private String s(long intValue) {
        assert intValue >= 0 && intValue < 10000 : intValue;
        return String.format("%04d", intValue);
    }

    private void validateHistogram(
            String tableName,
            String indexName,
            int expectedColumns,
            HistogramEntryDescription... entries
    ) {
        Index index =  PK.equals(indexName)
                ? getTable(SCHEMA, tableName).getPrimaryKey().getIndex()
                : getTable(SCHEMA, tableName).getIndex(indexName);
        validateHistogram(
                index, expectedColumns,
                entries
        );
    }

    private void validateHistogram(
            final Index index, int expectedColumns,
            HistogramEntryDescription... entries
    ) {
        final IndexStatisticsService statsService = statsService();
        if (analyzedIndexes.add(index)) {
            ddl().updateTableStatistics(
                    session(),
                    index.leafMostTable().getName(),
                    Collections.singleton(index.getIndexName().getName())
            );
        }

        if (entries != null) {
            IndexStatistics stats = statsService.getIndexStatistics(session(), index);
            Histogram histogram = stats.getHistogram(0, expectedColumns);

            assertEquals("histogram column count", expectedColumns, histogram.getColumnCount());
            List<HistogramEntry> actualEntries = histogram.getEntries();
            List<HistogramEntryDescription> expectedList = Arrays.asList(entries);
            AssertUtils.assertCollectionEquals("entries", expectedList, actualEntries);
        }
    }
    
    private HistogramEntryDescription entry(String keyString, long equalCount, long lessCount,
                                                            long distinctCount) {
        return new HistogramEntryDescription(keyString, equalCount, lessCount, distinctCount);
    }

    private int bucketCount() {
        return serviceManager().getServiceByClass(IndexStatisticsService.class).bucketCount();
    }

    private GroupIndex namePlacedGi;
    private Set<Index> analyzedIndexes = new HashSet<>();
    private int oidCounter = 0;

    private static final String SCHEMA = "indexes";
    private static final String PK = "PK";
    private static final int CUSTOMERS_COUNT = 320;
    private static final int ORDERS_COUNT = 3;
}
