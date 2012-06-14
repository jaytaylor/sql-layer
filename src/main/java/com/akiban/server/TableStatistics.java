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

package com.akiban.server;

import com.akiban.server.rowdata.RowData;

import java.util.ArrayList;
import java.util.List;

public class TableStatistics {

    public final static int LOW_CARDINALITY = 1;

    private final int rowDefId;

    private long rowCount;

    private long autoIncrementValue;

    private int meanRecordLength;

    private int blockSize;

    private long creationTime;

    private long updateTime;

    private List<Histogram> histograms = new ArrayList<Histogram>();

    public TableStatistics(final int rowDefId) {
        this.rowDefId = rowDefId;
    }

    public long getRowCount() {
        return rowCount;
    }

    public void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    public long getAutoIncrementValue() {
        return autoIncrementValue;
    }

    public void setAutoIncrementValue(long autoIncrementValue) {
        this.autoIncrementValue = autoIncrementValue;
    }

    public int getMeanRecordLength() {
        return meanRecordLength;
    }

    public void setMeanRecordLength(int meanRecordLength) {
        this.meanRecordLength = meanRecordLength;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(int blockSize) {
        this.blockSize = blockSize;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
    }

    public long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(long updateTime) {
        this.updateTime = updateTime;
    }

    public List<Histogram> getHistogramList() {
        return histograms;
    }

    public void addHistogram(Histogram histogram) {
        this.histograms.add(histogram);
    }

    public void clearHistograms() {
        this.histograms.clear();
    }

    public int getRowDefId() {
        return rowDefId;
    }

    public static class Histogram {

        private final int indexId;

        private List<HistogramSample> samples = new ArrayList<HistogramSample>();

        public Histogram(final int indexId) {
            this.indexId = indexId;

        }

        public int getIndexId() {
            return indexId;
        }

        public final List<HistogramSample> getHistogramSamples() {
            return samples;
        }

        public void addSample(final HistogramSample sample) {
            this.samples.add(sample);
        }

        public String toString() {
            final StringBuilder sb = new StringBuilder("Histogram(" + indexId
                    + ")[");
            boolean first = true;
            for (final HistogramSample sample : samples) {
                if (!first) {
                    sb.append(",");
                }
                sb.append(AkServerUtil.NEW_LINE);
                sb.append("  ");
                sb.append(sample);
            }
            sb.append(AkServerUtil.NEW_LINE);
            sb.append("]");
            return sb.toString();
        }
    }

    public static class HistogramSample {

        public HistogramSample(final RowData rowData, final long rowCount) {
            this.rowData = rowData;
            this.rowCount = rowCount;
        }

        private final RowData rowData;

        public RowData getRowData() {
            return rowData;
        }

        public long getRowCount() {
            return rowCount;
        }

        private final long rowCount;

        public String toString() {
            return String.format("%,8d->%s", rowCount, rowData.toString());
        }

    }
}
