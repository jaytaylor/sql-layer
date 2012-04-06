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

package com.akiban.server.test.it.store;

import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.Index;
import com.akiban.server.store.statistics.IndexStatistics;
import com.akiban.server.store.statistics.IndexStatisticsService;
import com.akiban.server.store.statistics.IndexStatistics.HistogramEntryDescription;
import com.akiban.server.store.statistics.PersistitIndexStatisticsVisitor;
import com.akiban.server.store.statistics.histograms.Sampler;
import com.akiban.server.test.it.ITBase;
import com.akiban.util.AssertUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

public final class IndexHistogramsIT extends ITBase {
    
    @Test
    public void customersPk() {
        HistogramEntryDescription[] expected = new HistogramEntryDescription[32];
        // there are 320 customers and 32 buckets, so 10 histograms per bucket. Each bucket is defined by its
        // *last* entry, and we're 0 based. So it's 0009, 0019, 0029...
        for (int i=0; i < expected.length; ++i) {
            int entryCid = 9 + 10*i;
            String entryString = String.format("{\"%04d\"}", entryCid);
            expected[i] = entry(entryString, 1, 9, 9);
        }
        validateHistogram("customers", PK, 1, expected);
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
        // 4), and the rest will be striped evenly. Each bucket will represent 8 entries: itself, and the 7 below it.
        // This algorithm is a bit tricky to pin down, so I'm putting the results in manually
        HistogramEntryDescription[] expected = new HistogramEntryDescription[31];
        int i = 0;
        expected[i++] = entry("{null,\"1983\"}", 80, 0, 0);
        expected[i++] = entry("{\"Bob\",\"1914\"}", 1, 7, 7);
        expected[i++] = entry("{\"Bob\",\"1930\"}", 1, 7, 7);
        expected[i++] = entry("{\"Bob\",\"1946\"}", 1, 7, 7);
        expected[i++] = entry("{\"Bob\",\"1962\"}", 1, 7, 7);
        expected[i++] = entry("{\"Bob\",\"1978\"}", 1, 7, 7);
        expected[i++] = entry("{\"Bob\",\"1994\"}", 1, 7, 7);
        expected[i++] = entry("{\"Bob\",\"2010\"}", 1, 7, 7);
        expected[i++] = entry("{\"Bob\",\"2026\"}", 1, 7, 7);
        expected[i++] = entry("{\"Bob\",\"2042\"}", 1, 7, 7);
        expected[i++] = entry("{\"Bob\",\"2058\"}", 1, 7, 7);
        expected[i++] = entry("{\"Carla\",\"1915\"}", 1, 7, 7);
        expected[i++] = entry("{\"Carla\",\"1931\"}", 1, 7, 7);
        expected[i++] = entry("{\"Carla\",\"1947\"}", 1, 7, 7);
        expected[i++] = entry("{\"Carla\",\"1963\"}", 1, 7, 7);
        expected[i++] = entry("{\"Carla\",\"1979\"}", 1, 7, 7);
        expected[i++] = entry("{\"Carla\",\"1995\"}", 1, 7, 7);
        expected[i++] = entry("{\"Carla\",\"2011\"}", 1, 7, 7);
        expected[i++] = entry("{\"Carla\",\"2027\"}", 1, 7, 7);
        expected[i++] = entry("{\"Carla\",\"2043\"}", 1, 7, 7);
        expected[i++] = entry("{\"Carla\",\"2059\"}", 1, 7, 7);
        expected[i++] = entry("{\"Dot\",\"1915\"}", 1, 7, 7);
        expected[i++] = entry("{\"Dot\",\"1931\"}", 1, 7, 7);
        expected[i++] = entry("{\"Dot\",\"1947\"}", 1, 7, 7);
        expected[i++] = entry("{\"Dot\",\"1963\"}", 1, 7, 7);
        expected[i++] = entry("{\"Dot\",\"1979\"}", 1, 7, 7);
        expected[i++] = entry("{\"Dot\",\"1995\"}", 1, 7, 7);
        expected[i++] = entry("{\"Dot\",\"2011\"}", 1, 7, 7);
        expected[i++] = entry("{\"Dot\",\"2027\"}", 1, 7, 7);
        expected[i++] = entry("{\"Dot\",\"2043\"}", 1, 7, 7);
        expected[i] = entry("{\"Dot\",\"2059\"}", 1, 7, 7);
        validateHistogram("customers", "name_dob", 2, expected);
    }
    
    @Test
    public void ordersPk() {
        HistogramEntryDescription[] expected = new HistogramEntryDescription[32];
        // there are 320*3 = 960 orders and 32 buckets, so 30 histograms per bucket. Each bucket is defined by its
        // *last* entry, and we're 0 based. So it's 29, 59, 89...
        for (int i=0; i < expected.length; ++i) {
            int entryCid = 29 + 30*i;
            String entryString = String.format("{(long)%d}", entryCid);
            expected[i] = entry(entryString, 1, 29, 29);
        }
        validateHistogram("orders", PK, 1, expected);
    }
    
    @Test
    public void ordersPlaced() {
        // "placed" is 0 if oid divisible by 5, 50 if divisible by 7 but not 5, oid otherwise.
        // There are 320+3=960 orders, of which:
        //   - 192 are divisible by 5
        //   - 137 are divisible by 7, but
        //   - 27 are divisible by 35, so
        //   - 137-27 = 110 are divisible by 7 but not 5
        // This leaves 658 values of the 10+oid class. These each have ~21 entries (658/32), and are spaced 30-32
        // elements apart (the difference in 20 entries and 30 values difference being due to the /5s and /7s.
        // There's not a simple equation for figuring out each entry, so we'll build up the expecteds algorithmically.
        List<HistogramEntryDescription> expected = new ArrayList<HistogramEntryDescription>();

        // div-by-5s first
        expected.add(entry("{\"0000\"}", 192, 0, 0));
        // build up every 20 entries, plus an entry for the end
        int lessThans = 0;
        int lessThansOffset = 0;
        int max = CUSTOMERS_COUNT * ORDERS_COUNT;
        for(int oid=0; oid < max; ++oid) {
            if (oid == 50) {
                expected.add(entry("{\"0050\"}", 110, lessThans, lessThans));
                lessThansOffset = lessThans;
                continue;
            }
            if (divisibleBy(oid, 5) || divisibleBy(oid, 7))
                continue;
            boolean hasEntry = (oid  == 958) || lessThans == 21; // last value is 559, but it's divisible by 7
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
        // 320 customers with 4 names, evenly distributed, is 80 customers per name
        // See ordersPlaced() for analysis for "placed"
        // Each name will have placed=0 192/4 = 48 times.
        // It will have placed=50 110/4=27.5 times
        // There are 658 remaining "placed" values, distributed evenly among the remaining buckets.
        // There are 32 buckets originally, minus the 8 for all the (name,0)s and (name,50)s, so 24 buckets. This means
        // the remaining buckets should each represent about 658/24 = ~27 buckets, plus or minus due to some buckets
        // being included in the less-thans of (name,0)s and (name,50)s.
        // I ran the tested, eyeballed the results, and copied them here.
        validateHistogram(namePlacedGi, 2,
                entry("{null,\"0000\"}", 48, 0, 0),     // important to get this right!
                entry("{null,\"0050\"}", 28, 10, 10),   // important to get this right!
                entry("{null,\"0156\"}", 1, 16, 16),
                entry("{null,\"0314\"}", 1, 26, 26),
                entry("{null,\"0458\"}", 1, 26, 26),
                entry("{null,\"0626\"}", 1, 26, 26),
                entry("{null,\"0792\"}", 1, 26, 26),
                entry("{null,\"0937\"}", 1, 26, 26),
                entry("{\"Bob\",\"0000\"}", 48, 2, 2),      // important to get this right!
                entry("{\"Bob\",\"0050\"}", 28, 8, 8),      // important to get this right!
                entry("{\"Bob\",\"0148\"}", 1, 16, 16),
                entry("{\"Bob\",\"0293\"}", 1, 26, 26),
                entry("{\"Bob\",\"0459\"}", 1, 26, 26),
                entry("{\"Bob\",\"0627\"}", 1, 26, 26),
                entry("{\"Bob\",\"0771\"}", 1, 26, 26),
                entry("{\"Bob\",\"0929\"}", 1, 26, 26),
                entry("{\"Carla\",\"0000\"}", 48, 4, 4),    // important to get this right!
                entry("{\"Carla\",\"0050\"}", 28, 8, 8),    // important to get this right!
                entry("{\"Carla\",\"0138\"}", 1, 14, 14),
                entry("{\"Carla\",\"0283\"}", 1, 26, 26),
                entry("{\"Carla\",\"0451\"}", 1, 26, 26),
                entry("{\"Carla\",\"0606\"}", 1, 26, 26),
                entry("{\"Carla\",\"0762\"}", 1, 26, 26),
                entry("{\"Carla\",\"0919\"}", 1, 26, 26),
                entry("{\"Dot\",\"0000\"}", 48, 6, 6),      // important to get this right!
                entry("{\"Dot\",\"0050\"}", 26, 8, 8),      // important to get this right!
                entry("{\"Dot\",\"0117\"}", 1, 12, 12),
                entry("{\"Dot\",\"0274\"}", 1, 26, 26),
                entry("{\"Dot\",\"0442\"}", 1, 26, 26),
                entry("{\"Dot\",\"0587\"}", 1, 26, 26),
                entry("{\"Dot\",\"0753\"}", 1, 26, 26),
                entry("{\"Dot\",\"0909\"}", 1, 26, 26),
                entry("{\"Dot\",\"0958\"}", 1, 9, 9)
        );
    }

    @Test
    public void edgeAnalysis() {
        int cTable = getUserTable(SCHEMA, "customers").getTableId();
        int oTable = getUserTable(SCHEMA, "orders").getTableId();
        int maxCid = PersistitIndexStatisticsVisitor.BUCKETS_COUNT * Sampler.OVERSAMPLE_FACTOR;
        insertRows(cTable, oTable, CUSTOMERS_COUNT, maxCid);

        HistogramEntryDescription[] expected = new HistogramEntryDescription[32];
        // there are 1600 customers and 32 buckets, so 50 histograms per bucket. Each bucket is defined by its
        // *last* entry, and we're 0 based. So it's 0049, 0099, 0149...
        for (int i=0; i < expected.length; ++i) {
            int entryCid = 49 + 50*i;
            String entryString = String.format("{\"%04d\"}", entryCid);
            expected[i] = entry(entryString, 1, 49, 49);
        }
        validateHistogram("customers", PK, 1, expected);
    }

    @Test
    public void largeAnalysis() {
        int cTable = getUserTable(SCHEMA, "customers").getTableId();
        int oTable = getUserTable(SCHEMA, "orders").getTableId();
        int maxCid = PersistitIndexStatisticsVisitor.BUCKETS_COUNT * Sampler.OVERSAMPLE_FACTOR+1;
        insertRows(cTable, oTable, CUSTOMERS_COUNT, maxCid);

        HistogramEntryDescription[] expected = new HistogramEntryDescription[33];
        // there are 1600 customers and 32 buckets, so 50 histograms per bucket. Each bucket is defined by its
        // *last* entry, and we're 0 based. So it's 0049, 0099, 0149...
        // In this case, we'll have 32 normal buckets, plus a "caboose" for the last entry
        for (int i=0; i < expected.length-1; ++i) {
            int entryCid = 49 + 50*i;
            String entryString = String.format("{\"%04d\"}", entryCid);
            expected[i] = entry(entryString, 1, 49, 49);
        }
        expected[32] = entry("{\"1600\"}", 1, 0, 0);
        validateHistogram("customers", PK, 1, expected);
    }

    @Test
    public void oversampleNotEvenlyDistributed() {
        int cTable = getUserTable(SCHEMA, "customers").getTableId();
        int oTable = getUserTable(SCHEMA, "orders").getTableId();
        double interval = 2.02;
        double oversamples = PersistitIndexStatisticsVisitor.BUCKETS_COUNT * Sampler.OVERSAMPLE_FACTOR;
        int maxCid = (int) Math.round(interval * oversamples);
        insertRows(cTable, oTable, CUSTOMERS_COUNT, maxCid);

        validateHistogram("customers", PK, 1, (HistogramEntryDescription)null);
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
        String groupName = getUserTable(SCHEMA, "customers").getGroup().getName();
        namePlacedGi = createGroupIndex(groupName, "namePlaced", "customers.name,orders.placed");

        // insert data
        int startingCid = 0;
        int endingCid = CUSTOMERS_COUNT;
        insertRows(cTable, oTable, startingCid, endingCid);
    }

    private void insertRows(int cTable, int oTable, int startingCid, int endingCid) {
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
                if (divisibleBy(oid, 5))
                    placed = 0;
                else if (divisibleBy(oid, 7))
                    placed = 50;
                else
                    placed = oid;
                writeRow(oTable, oid, s(cid), s(placed));
            }
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
                ? getUserTable(SCHEMA, tableName).getPrimaryKey().getIndex()
                : getUserTable(SCHEMA, tableName).getIndex(indexName);
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
            IndexStatistics.Histogram histogram = stats.getHistogram(expectedColumns);

            assertEquals("histogram column count", expectedColumns, histogram.getColumnCount());
            List<IndexStatistics.HistogramEntry> actualEntries = histogram.getEntries();
            List<HistogramEntryDescription> expectedList = Arrays.asList(entries);
            AssertUtils.assertCollectionEquals("entries", expectedList, actualEntries);
        }
    }
    
    private HistogramEntryDescription entry(String keyString, long equalCount, long lessCount,
                                                            long distinctCount) {
        return new HistogramEntryDescription(keyString, equalCount, lessCount, distinctCount);
    }

    private GroupIndex namePlacedGi;
    private Set<Index> analyzedIndexes = new HashSet<Index>();
    private int oidCounter = 0;

    private static final String SCHEMA = "indexes";
    private static final String PK = "PK";
    private static final int CUSTOMERS_COUNT = 320;
    private static final int ORDERS_COUNT = 3;
}
