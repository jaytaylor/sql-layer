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

import static org.junit.Assert.assertEquals;

import org.junit.Ignore;
import com.foundationdb.ais.model.Index;
import com.foundationdb.server.rowdata.RowDef;
import org.junit.Test;

import com.foundationdb.server.TableStatistics;

@Ignore("Too slow")
public class AnalyzeIndexIT extends AbstractScanBase {

    @Test
    public void testPopulateTableStatistics() throws Exception {
        final RowDef rowDef = rowDef("aa");
        final TableStatistics ts = dml().getTableStatistics(session(), rowDef.getRowDefId(), true);
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
            assertEquals(33, histogram.getHistogramSamples().size());
            assertEquals(100, histogram.getHistogramSamples().get(32)
                    .getRowCount());
        }
        {
            // Checks an hkeyEquivalent index
            //
            final int indexId = findIndexId(rowDef, Index.PRIMARY);
            TableStatistics.Histogram histogram = null;
            for (TableStatistics.Histogram h : ts.getHistogramList()) {
                if (h.getIndexId() == indexId) {
                    histogram = h;
                    break;
                }
            }
            assertEquals(33, histogram.getHistogramSamples().size());
            assertEquals(100, histogram.getHistogramSamples().get(32)
                    .getRowCount());
        }
    }

}
