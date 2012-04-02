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

import java.util.List;

/** Index statistics
 */
public class IndexStatistics
{
    private final Index index;
    private long analysisTimestamp, rowCount, sampledCount;
    private Histogram[] histograms; // Indexed by column count.

    protected IndexStatistics(Index index) {
        this.index = index;
        this.histograms = new Histogram[index.getKeyColumns().size()];
    }
    
    public Index index() {
        return index;
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

    public Histogram getHistogram(int columnCount) {
        return histograms[columnCount - 1];
    }

    protected void addHistogram(Histogram histogram) {
        assert (histograms[histogram.getColumnCount() - 1] == null);
        histograms[histogram.getColumnCount() - 1] = histogram;
    }
    
    public static class Histogram {
        private int columnCount;
        private List<HistogramEntry> entries;
        
        protected Histogram(int columnCount, List<HistogramEntry> entries) {
            this.columnCount = columnCount;
            this.entries = entries;
        }

        public int getColumnCount() {
            return columnCount;
        }
        public List<HistogramEntry> getEntries() {
            return entries;
        }

        public long totalDistinctCount() {
            long total = 0;
            for (HistogramEntry entry : entries) {
                if (entry.getEqualCount() > 0)
                    total++;
                total += entry.getDistinctCount();
            }
            return total;
        }

        @Override
        public String toString() {
            return toString(null);
        }

        public String toString(Index index) {
            StringBuilder str = new StringBuilder(getClass().getSimpleName());
            if (index != null) {
                str.append(" for ").append(index.getIndexName()).append("(");
                for (int j = 0; j < columnCount; j++) {
                    if (j > 0) str.append(", ");
                    str.append(index.getKeyColumns().get(j).getColumn().getName());
                }
                str.append("):\n");
            }
            str.append(entries);
            return str.toString();
        }

    }

    public static class HistogramEntry extends HistogramEntryDescription {
        private byte[] keyBytes;

        protected HistogramEntry(String keyString, byte[] keyBytes,
                                 long equalCount, long lessCount, long distinctCount) {
            super(keyString, equalCount, lessCount, distinctCount);
            this.keyBytes = keyBytes;
        }

        public byte[] getKeyBytes() {
            return keyBytes;
        }
    }
    
    public static class HistogramEntryDescription {

        protected String keyString;
        protected long equalCount;
        protected long lessCount;
        protected long distinctCount;

        public HistogramEntryDescription(String keyString, long equalCount, long lessCount, long distinctCount) {
            this.distinctCount = distinctCount;
            this.equalCount = equalCount;
            this.keyString = keyString;
            this.lessCount = lessCount;
        }

        /** A user-visible form of the key for this entry. */
        public String getKeyString() {
            return keyString;
        }

        /** The number of samples that were equal to the key value. */
        public long getEqualCount() {
            return equalCount;
        }

        /** The number of samples that were less than the key value
         * (and greater than the previous entry's key value, if any).
         */
        public long getLessCount() {
            return lessCount;
        }

        /** The number of distinct values in the less-than range. */
        public long getDistinctCount() {
            return distinctCount;
        }

        @Override
        final public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof HistogramEntryDescription)) return false;

            HistogramEntryDescription that = (HistogramEntryDescription) o;

            return distinctCount == that.distinctCount
                    && equalCount == that.equalCount
                    && lessCount == that.lessCount
                    && keyString.equals(that.keyString);

        }

        @Override
        final public int hashCode() {
            int result = keyString.hashCode();
            result = 31 * result + (int) (equalCount ^ (equalCount >>> 32));
            result = 31 * result + (int) (lessCount ^ (lessCount >>> 32));
            result = 31 * result + (int) (distinctCount ^ (distinctCount >>> 32));
            return result;
        }

        @Override
        public String toString() {
            return "{" + getKeyString() +
                    ": = " + getEqualCount() +
                    ", < " + getLessCount() +
                    ", distinct " + getDistinctCount() +
                    "}";
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
        for (int i = 0; i < histograms.length; i++) {
            Histogram h = histograms[i];
            if (h == null) continue;
            str.append("\n");
            str.append(h.toString(index));
        }
        return str.toString();
    }

}
