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

package com.akiban.server.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.akiban.ais.model.Index;
import org.junit.Test;

import com.akiban.server.IndexDef;
import com.akiban.server.RowData;
import com.akiban.server.RowDef;
import com.akiban.server.TableStatistics;

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
        final RowDef rowDef = rowDef("aa");
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
            final int indexId = findIndexId(rowDef, Index.PRIMARY_KEY_CONSTRAINT);
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
        final RowDef rowDef = rowDef("_akiban_a");
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
        final RowDef rowDef = rowDef("bug253");
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
//        final RowDef rowDef = groupRowDef("_akiban_srt");
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
