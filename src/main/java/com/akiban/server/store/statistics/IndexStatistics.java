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
    private long rowCount, sampledCount;
    private Histogram[] histograms; // Indexed by column count.

    protected IndexStatistics(Index index) {
        this.index = index;
        this.histograms = new Histogram[index.getColumns().size()];
    }

    public Index getIndex() {
        return index;
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
        return histograms[columnCount];
    }

    protected void setHistogram(int columnCount, Histogram histogram) {
        histograms[columnCount] = histogram;
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

        @Override
        public String toString() {
            return getClass().getSimpleName() + " for " + index + ": " + entries;
        }

    }

    public static class HistogramEntry {
        private String keyString;
        private byte[] keyBytes;
        private long equalCount, lessCount, distinctCount;

        protected HistogramEntry(String keyString, byte[] keyBytes,
                                 long equalCount, long lessCount, long DistinctCount) {
            this.keyString = keyString;
            this.keyBytes = keyBytes;
            this.equalCount = equalCount;
            this.lessCount = lessCount;
            this.distinctCount = distinctCount;
        }

        public String getKeyString() {
            return keyString;
        }
        public byte[] getKeyBytes() {
            return keyBytes;
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
        public String toString() {
            return "{" + keyString +
                ": = " + equalCount +
                ", < " + lessCount +
                ", dist " + distinctCount +
                "}";
        }
    }

}
