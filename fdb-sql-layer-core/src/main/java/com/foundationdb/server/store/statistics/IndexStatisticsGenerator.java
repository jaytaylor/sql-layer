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
import com.foundationdb.server.store.statistics.histograms.Bucket;
import com.foundationdb.server.store.statistics.histograms.Sampler;
import com.foundationdb.util.Flywheel;

import java.util.ArrayList;
import java.util.List;

abstract class IndexStatisticsGenerator<K extends Comparable<? super K>, V>
{
    private final Flywheel<K> keysFlywheel;
    private final int columnCount;
    private final int singleColumnPosition; // -1 for multi-column
    private Sampler<K> keySampler;
    protected final Index index;
    protected final long timestamp;
    protected int rowCount;

    protected IndexStatisticsGenerator(Flywheel<K> keysFlywheel,
                                       Index index,
                                       int columnCount,
                                       int singleColumnPosition) {
        this.keysFlywheel = keysFlywheel;
        this.index = index;
        this.columnCount = columnCount;
        this.singleColumnPosition = singleColumnPosition;
        this.timestamp = System.currentTimeMillis();
        this.rowCount = 0;
    }


    //
    // IndexStatisticsGenerator, derived
    //

    protected abstract byte[] copyOfKeyBytes(K key);

    protected abstract Sampler<K> createKeySampler(int bucketCount, long distinctCount);

    protected final Flywheel<K> getKeysFlywheel() {
        return keysFlywheel;
    }

    protected final void loadKey(K key) {
        List<? extends K> recycles = keySampler.visit(key);
        rowCount++;
        for(K recycle : recycles) {
            keysFlywheel.recycle(recycle);
        }
    }


    //
    // IndexStatisticsGenerator, public
    //

    public abstract void visit(K key, V value);

    public final int columnCount() {
        return columnCount;
    }

    public final int rowCount() {
        return rowCount;
    }

    public void init(int bucketCount, long distinctCount) {
        this.keySampler = createKeySampler(bucketCount, distinctCount);
        keySampler.init();
    }

    public void finish(int bucketCount) {
        keySampler.finish();
    }

    public final void getIndexStatistics(IndexStatistics indexStatistics) {
        indexStatistics.setAnalysisTimestamp(timestamp);
        List<List<Bucket<K>>> segmentBuckets = keySampler.toBuckets();
        assert segmentBuckets.size() == columnCount
            : String.format("expected %s segments, saw %s: %s", columnCount, segmentBuckets.size(), segmentBuckets);
        for (int colCountSegment = 0; colCountSegment < columnCount; colCountSegment++) {
            List<Bucket<K>> segmentSamples = segmentBuckets.get(colCountSegment);
            int samplesCount = segmentSamples.size();
            List<HistogramEntry> entries = new ArrayList<>(samplesCount);
            for (Bucket<K> sample : segmentSamples) {
                K key = sample.value();
                byte[] keyBytes = copyOfKeyBytes(key);
                HistogramEntry entry = new HistogramEntry(
                    key.toString(),
                    keyBytes,
                    sample.getEqualsCount(),
                    sample.getLessThanCount(),
                    sample.getLessThanDistinctsCount()
                );
                entries.add(entry);
            }
            if (singleColumnPosition < 0) {
                indexStatistics.addHistogram(new Histogram(0, colCountSegment + 1, entries));
            } else if (singleColumnPosition > 0) {
                indexStatistics.addHistogram(new Histogram(singleColumnPosition, 1, entries));
            } else {
                // Single-column histogram for leading column is handled as a multi-column
                // histogram with column count = 1.
                assert false : "unnecesary sampler " + this;
            }
        }
    }
}
