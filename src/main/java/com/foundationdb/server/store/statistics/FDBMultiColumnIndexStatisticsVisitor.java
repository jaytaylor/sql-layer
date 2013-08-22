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
import com.foundationdb.server.service.tree.KeyCreator;
import com.foundationdb.server.store.statistics.histograms.Sampler;
import com.persistit.Key;

public class FDBMultiColumnIndexStatisticsVisitor extends IndexStatisticsGenerator<Key,byte[]>
{
    public FDBMultiColumnIndexStatisticsVisitor(Index index, KeyCreator keyCreator) {
        super(new PersistitKeyFlywheel(keyCreator), index, index.getKeyColumns().size(), -1);
    }

    @Override
    public void visit(Key key, byte[] value) {
        loadKey(key);
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
