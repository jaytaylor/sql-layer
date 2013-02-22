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
import com.akiban.server.service.tree.KeyCreator;
import com.akiban.server.store.statistics.histograms.Bucket;
import com.akiban.server.store.statistics.histograms.Sampler;
import com.akiban.server.store.statistics.histograms.Splitter;
import com.akiban.util.Flywheel;
import com.persistit.Key;
import com.persistit.Value;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

abstract class IndexStatisticsGenerator
{
    protected final Index index;
    private final int columnCount;
    private final int singleColumnPosition; // -1 for multi-column
    protected final long timestamp;
    protected int rowCount;
    private final KeyCreator keyCreator;
    private Sampler<Key> keySampler;
    private final Flywheel<Key> keysFlywheel = new Flywheel<Key>() {
        @Override
        protected Key createNew() {
            return keyCreator.createKey();
        }
    };

    public int rowCount()
    {
        return rowCount;
    }

    protected IndexStatisticsGenerator(Index index,
                                       int columnCount,
                                       int singleColumnPosition,
                                       KeyCreator keyCreator) {
        this.index = index;
        this.keyCreator = keyCreator;
        this.columnCount = columnCount;
        this.singleColumnPosition = singleColumnPosition;
        this.timestamp = System.currentTimeMillis();
        this.rowCount = 0;
    }
    
    private static class KeySplitter implements Splitter<Key> {
        @Override
        public int segments() {
            return keys.size();
        }

        @Override
        public List<? extends Key> split(Key keyToSample) {
            Key prev = keyToSample;
            for (int i = keys.size() ; i > 0; i--) {
                Key truncatedKey = keysFlywheel.get();
                prev.copyTo(truncatedKey);
                truncatedKey.setDepth(i);
                keys.set(i-1 , truncatedKey);
                prev = truncatedKey;
            }
            return keys;
        }

        private KeySplitter(int columnCount, Flywheel<Key> keysFlywheel) {
            keys = Arrays.asList(new Key[columnCount]);
            this.keysFlywheel = keysFlywheel;
        }

        private List<Key> keys;
        private Flywheel<Key> keysFlywheel;
    }
    
    public void init(int bucketCount, long distinctCount) {
        this.keySampler =
            new Sampler<>(new KeySplitter(columnCount, keysFlywheel),
                             bucketCount,
                             distinctCount,
                             keysFlywheel);
        keySampler.init();
    }

    public abstract void visit(Key key, Value value);

    public void finish(int bucketCount) {
        keySampler.finish();
    }

    protected final void loadKey(Key key)
    {
        List<? extends Key> recycles = keySampler.visit(key);
        rowCount++;
        for (int i = 0, len = recycles.size(); i < len; ++i) {
            keysFlywheel.recycle(recycles.get(i));
        }
    }

    public final void getIndexStatistics(IndexStatistics indexStatistics) {
        indexStatistics.setAnalysisTimestamp(timestamp);
        List<List<Bucket<Key>>> segmentBuckets = keySampler.toBuckets();
        assert segmentBuckets.size() == columnCount
            : String.format("expected %s segments, saw %s: %s", columnCount, segmentBuckets.size(), segmentBuckets);
        for (int colCountSegment = 0; colCountSegment < columnCount; colCountSegment++) {
            List<Bucket<Key>> segmentSamples = segmentBuckets.get(colCountSegment);
            int samplesCount = segmentSamples.size();
            List<HistogramEntry> entries = new ArrayList<>(samplesCount);
            for (Bucket<Key> sample : segmentSamples) {
                Key key = sample.value();
                byte[] keyBytes = new byte[key.getEncodedSize()];
                System.arraycopy(key.getEncodedBytes(), 0, keyBytes, 0, keyBytes.length);
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
            } // else: Single-column histogram for leading column is handled as a multi-column histogram with
              // column count = 1.
        }
    }
}
