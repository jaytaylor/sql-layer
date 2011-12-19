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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.akiban.ais.model.Index;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import org.junit.BeforeClass;
import org.junit.Test;

import com.akiban.server.TableStatistics;

public class AnalyzeIndexIT extends AbstractScanBase {
    
    @BeforeClass
    static public void setUpTest() throws Exception {
        for (final RowDef rowDef : rowDefCache.getRowDefs()) {
            if(rowDef.isUserTable()) {
                store.analyzeTable(session, rowDef.getRowDefId(), 10);
            }
        }
    }

    @Test
    public void testPopulateTableStatistics() throws Exception {
        final RowDef rowDef = rowDef("aa");
        store.analyzeTable(session, rowDef.getRowDefId());
        final TableStatistics ts = serviceManager.getDXL().dmlFunctions().getTableStatistics(session, rowDef.getRowDefId(), false);
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
        final TableStatistics ts = serviceManager.getDXL().dmlFunctions().getTableStatistics(session, rowDef.getRowDefId(), false);
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
}
