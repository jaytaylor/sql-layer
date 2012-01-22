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

package com.akiban.server.store.statistics;

import com.akiban.ais.model.Index;

import java.util.List;

/** Index statistics
 */
public class IndexStatistics
{
    private Index index;
    private long analysisTimestamp, rowCount, sampledCount;
    private Histogram[] histograms; // Indexed by column count.

    protected IndexStatistics(Index index) {
        this.index = index;
        this.histograms = new Histogram[index.getColumns().size()];
    }

    public Index getIndex() {
        return index;
    }

    public long getAnalysisTimestamp() {
        return analysisTimestamp;
    }
    public void setAnalysisTimestamp(long analysisTimestamp) {
        this.analysisTimestamp = analysisTimestamp;
    }

    public long getRowCount() {
        return rowCount;
    }
    protected void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }

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
        private Index index;
        private int columnCount;
        private List<HistogramEntry> entries;
        
        protected Histogram(Index index, int columnCount, List<HistogramEntry> entries) {
            this.index = index;
            this.columnCount = columnCount;
            this.entries = entries;
        }

        public Index getIndex() {
            return index;
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
            StringBuilder str = new StringBuilder(getClass().getSimpleName());
            str.append(" for ").append(index.getIndexName()).append("(");
            for (int j = 0; j < columnCount; j++) {
                if (j > 0) str.append(", ");
                str.append(index.getColumns().get(j).getColumn().getName());
            }
            str.append("):\n");
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

        public String getKeyString() {
            return keyString;
        }

        public long getEqualCount() {
            return equalCount;
        }

        public long getLessCount() {
            return lessCount;
        }

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
        StringBuilder str = new StringBuilder(super.toString());
        str.append(" for ").append(index);
        for (int i = 0; i < histograms.length; i++) {
            Histogram h = histograms[i];
            if (h == null) continue;
            str.append("\n");
            str.append(h);
        }
        return str.toString();
    }

}
