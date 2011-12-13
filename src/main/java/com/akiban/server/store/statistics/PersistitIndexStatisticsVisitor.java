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

import static com.akiban.server.store.statistics.IndexStatistics.*;

import com.akiban.ais.model.Index;
import com.akiban.server.store.IndexVisitor;

import com.persistit.Key;
import com.persistit.Value;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/** Analyze index exhaustively by visiting every key.
 */
public class PersistitIndexStatisticsVisitor extends IndexVisitor
{
    private static final Logger logger = LoggerFactory.getLogger(PersistitIndexStatisticsVisitor.class);
    
    private Index index;
    private int columnCount;
    private Bucket[] buckets;
    private long timestamp;
    private int rowCount;
    private Key sampleKey;

    public PersistitIndexStatisticsVisitor(Index index) {
        this.index = index;
        
        columnCount = index.getColumns().size();
        buckets = new Bucket[columnCount];
        for (int i = 0; i < columnCount; i++) {
            buckets[i] = new Bucket(index, i + 1);
        }
        timestamp = System.currentTimeMillis();
        rowCount = 0;
    }

    protected void visit(Key key, Value value) {
        if (sampleKey == null)
            sampleKey = new Key(key);
        else
            key.copyTo(sampleKey);
        
        sampleKey.setDepth(columnCount);
        logger.debug("Key = " + sampleKey);

        for (int i = columnCount - 1; i >= 0; i--) {
            buckets[i].sample(sampleKey);
            sampleKey.setDepth(i);
        }

        rowCount++;
    }

    public IndexStatistics getIndexStatistics() {
        IndexStatistics result = new IndexStatistics(index);
        result.setAnalysisTimestamp(timestamp);
        result.setRowCount(rowCount);
        result.setSampledCount(rowCount);
        for (int i = 0; i < columnCount; i++) {
            result.addHistogram(buckets[i].getHistogram());
        }
        return result;
    }

    static class Bucket {
        private Index index;
        private int columnCount;

        private Key key;
        private long eqCount;
        private List<HistogramEntry> entries = new ArrayList<HistogramEntry>();

        public Bucket(Index index, int columnCount) {
            this.index = index;
            this.columnCount = columnCount;
        }

        public void sample(Key sampleKey) {
            if (key == null) {
                key = new Key(sampleKey);
                eqCount = 1;
            }
            else if (key.equals(sampleKey)) {
                eqCount++;
            }
            else {
                flush();
                sampleKey.copyTo(key);
                eqCount = 1;
            }
        }

        public Histogram getHistogram() {
            flush();
            return new Histogram(index, columnCount, entries);
        }

        private void flush() {
            byte[] keyBytes = new byte[key.getEncodedSize()];
            System.arraycopy(key.getEncodedBytes(), 0, keyBytes, 0, keyBytes.length);
            HistogramEntry entry = new HistogramEntry(key.toString(), keyBytes,
                                                      eqCount, 0, 0);
            entries.add(entry);
        }
    }

}
