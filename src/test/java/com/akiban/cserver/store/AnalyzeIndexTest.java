package com.akiban.cserver.store;

import static org.junit.Assert.assertEquals;

import org.junit.BeforeClass;
import org.junit.Test;

import com.akiban.cserver.IndexDef;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.TableStatistics;

public class AnalyzeIndexTest extends AbstractScanBase {

    // TODO - temporary hack - remove when head always wants the
    // histograms.
    @BeforeClass
    public static void setUp() {
        PersistitStoreIndexManager.enableHistograms = true;
    }

    @Test
    public void testAnalyzeAllIndexes() throws Exception {
        for (final RowDef rowDef : rowDefCache.getRowDefs()) {
            for (final IndexDef indexDef : rowDef.getIndexDefs()) {
                store.getIndexManager().analyzeIndex(indexDef, 10);
            }
        }
    }

    @Test
    public void testPopulateTableStatistics() throws Exception {
        final RowDef rowDef = userRowDef("aa");
        store.analyzeTable(rowDef.getRowDefId());
        final TableStatistics ts = new TableStatistics(rowDef.getRowDefId());
        store.getIndexManager().populateTableStatistics(ts);
        {
            // Checks a secondary index
            //
            final int indexId = findIndexId(rowDef, "str");
            TableStatistics.Histogram histogram = null;
            for (TableStatistics.Histogram h : ts.getHistogramList()) {
                if (h.getIndexId() == indexId) {
                    histogram = h;
                    break;
                }
            }
            assertEquals(32, histogram.getHistogramSamples().size());
            assertEquals(100, histogram.getHistogramSamples().get(31)
                    .getRowCount());
        }
        {
            // Checks an hkeyEquivalent index
            //
            final int indexId = findIndexId(rowDef, "PRIMARY");
            TableStatistics.Histogram histogram = null;
            for (TableStatistics.Histogram h : ts.getHistogramList()) {
                if (h.getIndexId() == indexId) {
                    histogram = h;
                    break;
                }
            }
            assertEquals(32, histogram.getHistogramSamples().size());
            assertEquals(100, histogram.getHistogramSamples().get(31)
                    .getRowCount());
        }
    }

    @Test
    public void testGroupTableStatistics() throws Exception {
        final RowDef rowDef = groupRowDef("_akiba_srt");
        store.analyzeTable(rowDef.getRowDefId());
        final TableStatistics ts = new TableStatistics(rowDef.getRowDefId());
        store.getIndexManager().populateTableStatistics(ts);
        final int indexId = findIndexId(rowDef, "aa$str");
        TableStatistics.Histogram histogram = null;
        for (TableStatistics.Histogram h : ts.getHistogramList()) {
            if (h.getIndexId() == indexId) {
                histogram = h;
                break;
            }
        }
        assertEquals(32, histogram.getHistogramSamples().size());
    }
}
