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

package com.akiban.server.store.statistics;

import com.akiban.ais.model.Index;

/** Index statistics
 */
public class IndexStatistics
{
    // NOTE: There is no backpointer to the Index in this class because of 
    // IndexStatisticsServiceImpl.cache, a WeakHashMap<Index,IndexStatistics>.
    private long analysisTimestamp, rowCount, sampledCount;
    // Single-column histograms are indexed by column position, starting at 1.
    // Multi-column histograms are indexed by (number of columns) - 1.
    // The histogram for the leading column of a multi-column index could be handled as single or multi. It
    // is handled only as a multi-column histogram for historical reasons, (single-column was added later).
    // Example: For an index on (a, b, c) we have the following histograms:
    //    singleColumnHistograms[0]: null
    //    singleColumnHistograms[1]: (b)
    //    singleColumnHistograms[2]: (c)
    //    multiColumnHistograms[0]: (a)
    //    multiColumnHistograms[1]: (a, b)
    //    multiColumnHistograms[2]: (a, b, c)
    private Histogram[] multiColumnHistograms;
    private Histogram[] singleColumnHistograms;

    protected IndexStatistics(Index index) {
        this.multiColumnHistograms = new Histogram[index.getKeyColumns().size()];
        this.singleColumnHistograms = new Histogram[index.getKeyColumns().size()];
    }
    
    /** The system time at which the statistics were gathered. */
    public long getAnalysisTimestamp() {
        return analysisTimestamp;
    }
    public void setAnalysisTimestamp(long analysisTimestamp) {
        this.analysisTimestamp = analysisTimestamp;
    }

    /** The number of rows in the index when it was analyzed. */
    public long getRowCount() {
        return rowCount;
    }
    protected void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    /** The number of rows that were actually sampled.
     * Right now, always equal to <code>rowCount</code>.
     */
    public long getSampledCount() {
        return sampledCount;
    }
    protected void setSampledCount(long sampledCount) {
        this.sampledCount = sampledCount;
    }

    public Histogram getHistogram(int firstColumn, int columnCount) {
        return
            firstColumn == 0
            ? multiColumnHistograms[columnCount - 1]
            : singleColumnHistograms[firstColumn];
    }

    protected void addHistogram(Histogram histogram) {
        if (histogram.getFirstColumn() == 0) {
            assert (multiColumnHistograms[histogram.getColumnCount() - 1] == null);
            multiColumnHistograms[histogram.getColumnCount() - 1] = histogram;
        } else {
            assert (singleColumnHistograms[histogram.getFirstColumn()] == null);
            singleColumnHistograms[histogram.getFirstColumn()] = histogram;
        }
    }

    @Override
    public String toString() {
        return toString(null);
    }

    public String toString(Index index) {
        StringBuilder str = new StringBuilder(super.toString());
        if (index != null)
            str.append(" for ").append(index);
        for (int i = 0; i < singleColumnHistograms.length; i++) {
            Histogram h = singleColumnHistograms[i];
            if (h == null) continue;
            str.append("\n");
            str.append(h.toString(index));
        }
        // Don't bother with multiColumnHistograms[0]. Same thing as singleColumnHistograms[0].
        for (int i = 1; i < multiColumnHistograms.length; i++) {
            Histogram h = multiColumnHistograms[i];
            if (h == null) continue;
            str.append("\n");
            str.append(h.toString(index));
        }
        return str.toString();
    }

}
