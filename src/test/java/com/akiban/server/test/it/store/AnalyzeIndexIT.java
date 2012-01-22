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
import com.persistit.Transaction;
import com.persistit.exception.PersistitException;
import org.junit.BeforeClass;
import org.junit.Test;

import com.akiban.server.TableStatistics;

public class AnalyzeIndexIT extends AbstractScanBase {

    @Test
    public void testPopulateTableStatistics() throws Exception {
        final RowDef rowDef = rowDef("aa");
        final TableStatistics ts = serviceManager.getDXL().dmlFunctions().getTableStatistics(session, rowDef.getRowDefId(), true);
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
            assertEquals(35, histogram.getHistogramSamples().size());
            assertEquals(99, histogram.getHistogramSamples().get(32)
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
            assertEquals(35, histogram.getHistogramSamples().size());
            assertEquals(99, histogram.getHistogramSamples().get(32)
                    .getRowCount());
        }
    }

}
