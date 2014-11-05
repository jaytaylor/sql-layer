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

import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.server.service.tree.KeyCreator;
import com.foundationdb.server.store.statistics.histograms.Sampler;
import com.persistit.Key;

import java.util.Map;
import java.util.TreeMap;

public class FDBSingleColumnIndexStatisticsVisitor extends IndexStatisticsGenerator<Key,byte[]>
{
    private final Key extractKey = new Key(null, Key.MAX_KEY_LENGTH);
    private final Map<Key,int[]> countMap = new TreeMap<>(); // Is ordered required?
    private final int field;

    public FDBSingleColumnIndexStatisticsVisitor(KeyCreator keyCreator, IndexColumn indexColumn)
    {
        super(new PersistitKeyFlywheel(keyCreator), indexColumn.getIndex(), 1, indexColumn.getPosition());
        this.field = indexColumn.getPosition();
    }


    @Override
    public void init(int bucketCount, long expectedRowCount) {
        // Does not do generator init until finish because just
        // buffering counts in this pass.
    }

    @Override
    public void finish(int bucketCount) {
        // Now init the generator and replay the accumulated counts.
        super.init(bucketCount, rowCount);
        try {
            for(Map.Entry<Key,int[]> entry : countMap.entrySet()) {
                int keyCount = entry.getValue()[0];
                for(int i = 0; i < keyCount; ++i) {
                    loadKey(entry.getKey());
                }
            }
        } finally {
            super.finish(bucketCount);
        }
    }

    @Override
    public void visit(Key key, byte[] value) {
        key.indexTo(field);
        extractKey.clear().appendKeySegment(key);
        int[] curCount = countMap.get(extractKey);
        if(curCount == null) {
            Key storedKey = new Key(extractKey);
            curCount = new int[1];
            countMap.put(storedKey, curCount);
        }
        curCount[0] += 1;
        rowCount++;
    }

    @Override
    public Sampler<Key> createKeySampler(int bucketCount, long distinctCount) {
        return new Sampler<>(
                new PersistitKeySplitter(columnCount(), getKeysFlywheel()),
                bucketCount,
                distinctCount,
                getKeysFlywheel()
        );
    }

    @Override
    protected byte[] copyOfKeyBytes(Key key) {
        byte[] copy = new byte[key.getEncodedSize()];
        System.arraycopy(key.getEncodedBytes(), 0, copy, 0, copy.length);
        return copy;
    }
}
