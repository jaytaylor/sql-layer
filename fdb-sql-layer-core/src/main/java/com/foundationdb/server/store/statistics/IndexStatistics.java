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

package com.foundationdb.server.store.statistics;

import com.foundationdb.ais.model.Index;

/** Index statistics
 */
public class IndexStatistics
{
    public static enum Validity { VALID, INVALID, OUTDATED };

    private final String indexName;
    // NOTE: There is no backpointer to the Index in this class because of
    // IndexStatisticsServiceImpl.cache, a WeakHashMap<Index,IndexStatistics>.
    private long analysisTimestamp, rowCount, sampledCount;
    private Validity validity;
    private boolean warned;
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
        this.indexName = index.getIndexName().getName();
        this.validity = Validity.VALID;
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

    public Validity getValidity() {
        return validity;
    }
    public void setValidity(Validity validity) {
        this.validity = validity;
    }
    public boolean isValid() {
        return (validity == Validity.VALID);
    }
    public boolean isInvalid() {
        return (validity == Validity.INVALID);
    }

    public boolean isWarned() {
        return warned;
    }
    public void setWarned(boolean warned) {
        this.warned = warned;
    }

    public Histogram getHistogram(int firstColumn, int columnCount) {
        assert firstColumn == 0 || columnCount == 1;
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
        histogram.setIndexStatistics(this);
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
        for (int i = 0; i < multiColumnHistograms.length; i++) {
            Histogram h = multiColumnHistograms[i];
            if (h == null) continue;
            str.append("\n");
            str.append(h.toString(index));
        }
        return str.toString();
    }

}
