
package com.akiban.server.test.it.store;

import static org.junit.Assert.assertEquals;

import com.akiban.ais.model.Index;
import com.akiban.server.rowdata.RowDef;
import org.junit.Test;

import com.akiban.server.TableStatistics;

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
            final int indexId = findIndexId(rowDef, Index.PRIMARY_KEY_CONSTRAINT);
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
