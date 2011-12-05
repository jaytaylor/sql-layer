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
    private Histogram[] histograms;
    private Key[] keys;
    private long[] counts;
    private long timestamp;
    private int rowCount;
    private Key sampleKey;

    public PersistitIndexStatisticsVisitor(Index index) {
        this.index = index;
        
        columnCount = index.getColumns().size();
        histograms = new Histogram[columnCount];
        for (int i = 0; i < columnCount; i++) {
            histograms[i] = new Histogram(index, i + 1, new ArrayList<HistogramEntry>());
        }
        keys = new Key[columnCount];
        counts = new long[columnCount];
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
            if (keys[i] == null) {
                keys[i] = new Key(sampleKey);
                counts[i] = 1;
            }
            else if (keys[i].equals(sampleKey)) {
                counts[i]++;
            }
            else {
                flush(i);
                sampleKey.copyTo(keys[i]);
                counts[i] = 1;
            }
            
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
            flush(i);
            result.addHistogram(histograms[i]);
        }
        return result;
    }

    private void flush(int i) {
        Key key = keys[i];
        byte[] keyBytes = new byte[key.getEncodedSize()];
        System.arraycopy(key.getEncodedBytes(), 0, keyBytes, 0, keyBytes.length);
        HistogramEntry entry = new HistogramEntry(key.toString(), keyBytes,
                                                  counts[i], 0, 0);
        histograms[i].getEntries().add(entry);
    }

}
