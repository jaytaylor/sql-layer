package com.akiban.cserver.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.akiban.cserver.IndexDef;
import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.TableStatistics;

public class AnalyzeIndexTest extends AbstractScanBase {
    
    @Test
    public void testAnalyzeAllIndexes() throws Exception {
        for (final RowDef rowDef : rowDefCache.getRowDefs()) {
            for (final IndexDef indexDef : rowDef.getIndexDefs()) {
                store.getIndexManager().analyzeIndex(session, indexDef, 10);
            }
        }
    }

    @Test
    public void testPopulateTableStatistics() throws Exception {
        final RowDef rowDef = userRowDef("aa");
        store.analyzeTable(session, rowDef.getRowDefId());
        final TableStatistics ts = new TableStatistics(rowDef.getRowDefId());
        store.getIndexManager().populateTableStatistics(session, ts);
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
        final RowDef rowDef = groupRowDef("_akiba_a");
        store.analyzeTable(session, rowDef.getRowDefId());
        final TableStatistics ts = new TableStatistics(rowDef.getRowDefId());
        store.getIndexManager().populateTableStatistics(session, ts);
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
    
    @Test
    public void testBug253() throws Exception {
        final RowDef rowDef = userRowDef("bug253");
        final RowData rowData = new RowData(new byte[256]);
        rowData.createRow(rowDef, new Object[]{1, "blog"});
        store.writeRow(session, rowData);
        rowData.createRow(rowDef, new Object[]{1, "book"});
        store.writeRow(session, rowData);
        try {
        store.analyzeTable(session, rowDef.getRowDefId());
        } catch (NumberFormatException nfe) {
            fail("Bug 253 strikes again!");
        }
    }
    
// This test breaks the build - need to populate and then drop a different table.
//    @Test
//    public void testDropTable() throws Exception {
//        final RowDef rowDef = groupRowDef("_akiba_srt");
//        store.analyzeTable(rowDef.getRowDefId());
//        for (final RowDef userRowDef : rowDef.getUserTableRowDefs()) {
//            store.analyzeTable(userRowDef.getRowDefId());
//        }
//        final TableStatistics ts1 = new TableStatistics(rowDef.getRowDefId());
//        store.getIndexManager().populateTableStatistics(ts1);
//        assertTrue(!ts1.getHistogramList().isEmpty());
//        for (final RowDef userRowDef : rowDef.getUserTableRowDefs()) {
//            store.dropTable(userRowDef.getRowDefId());
//        }
//        final TableStatistics ts2 = new TableStatistics(rowDef.getRowDefId());
//        store.getIndexManager().populateTableStatistics(ts2);
//        assertTrue(ts2.getHistogramList().isEmpty());
//    }
}
