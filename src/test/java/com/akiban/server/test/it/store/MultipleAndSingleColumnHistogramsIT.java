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

import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.TableName;
import com.akiban.server.store.statistics.Histogram;
import com.akiban.server.store.statistics.HistogramEntry;
import com.akiban.server.store.statistics.IndexStatistics;
import com.akiban.server.store.statistics.IndexStatisticsService;
import com.akiban.server.test.it.ITBase;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class MultipleAndSingleColumnHistogramsIT extends ITBase
{
    @Before
    public void createDatabase()
    {
        int table = createTable(SCHEMA, TABLE,
                                "id int not null",
                                "a int",
                                "b int",
                                "c int",
                                "primary key(id)");
        index = createIndex(SCHEMA, TABLE, INDEX, "a", "b", "c");
        int a = 0;
        int b = 0;
        int c = 0;
        for (int id = 0; id < N; id++) {
            writeRow(table, id, a, b, c);
            a = (a + 1) % A_COUNT;
            b = (b + 1) % B_COUNT;
            c = (c + 1) % C_COUNT;
        }
        ddl().updateTableStatistics(session(),
                                    TableName.create(SCHEMA, TABLE),
                                    Collections.singleton(index.getIndexName().getName()));
    }

    @Test
    public void test()
    {
        IndexStatisticsService statsService = statsService();
        IndexStatistics stats = statsService.getIndexStatistics(session(), index);
        Histogram histogram;
        List<HistogramEntry> entries;
        // Multi-column histogram on (a)
        histogram = stats.getHistogram(0, 1);
        assertEquals(0, histogram.getFirstColumn());
        assertEquals(1, histogram.getColumnCount());
        assertEquals(A_COUNT, histogram.totalDistinctCount());
        entries = histogram.getEntries();
        int a = 0;
        for (HistogramEntry entry : entries) {
            assertEquals(String.format("{(long)%s}", a++), entry.getKeyString());
            assertEquals(N / A_COUNT, entry.getEqualCount());
            assertEquals(0, entry.getDistinctCount());
            assertEquals(0, entry.getLessCount());
        }
        // Not checking other multi-column entries in detail, because there's nothing new, and they're pretty useless.
        // Single-column histogram on (b)
        histogram = stats.getHistogram(1, 1);
        assertEquals(1, histogram.getFirstColumn());
        assertEquals(1, histogram.getColumnCount());
        assertEquals(B_COUNT, histogram.totalDistinctCount());
        entries = histogram.getEntries();
        int b = 0;
        for (HistogramEntry entry : entries) {
            assertEquals(String.format("{(long)%s}", b++), entry.getKeyString());
            assertEquals(N / B_COUNT, entry.getEqualCount());
            assertEquals(0, entry.getDistinctCount());
            assertEquals(0, entry.getLessCount());
        }
        // Single-column histogram on (c)
        histogram = stats.getHistogram(2, 1);
        assertEquals(2, histogram.getFirstColumn());
        assertEquals(1, histogram.getColumnCount());
        assertEquals(C_COUNT, histogram.totalDistinctCount());
        entries = histogram.getEntries();
        int c = 0;
        for (HistogramEntry entry : entries) {
            assertEquals(String.format("{(long)%s}", c++), entry.getKeyString());
            assertEquals(N / C_COUNT, entry.getEqualCount());
            assertEquals(0, entry.getDistinctCount());
            assertEquals(0, entry.getLessCount());
        }
    }

    private IndexStatisticsService statsService()
    {
        return serviceManager().getServiceByClass(IndexStatisticsService.class);
    }

    private static final String SCHEMA = "schema";
    private static final String TABLE = "t";
    private static final String INDEX = "idx_abc";
    private static final int N = 1000;
    private static final int A_COUNT = 5;
    private static final int B_COUNT = 10;
    private static final int C_COUNT = 25;

    private TableIndex index;
}
