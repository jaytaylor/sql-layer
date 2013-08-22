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

import com.foundationdb.server.store.statistics.histograms.Splitter;
import com.foundationdb.util.Flywheel;
import com.persistit.Key;

import java.util.Arrays;
import java.util.List;

public class PersistitKeySplitter implements Splitter<Key> {
    private final List<Key> keys;
    private final Flywheel<Key> keysFlywheel;

    public PersistitKeySplitter(int columnCount, Flywheel<Key> keysFlywheel) {
        keys = Arrays.asList(new Key[columnCount]);
        this.keysFlywheel = keysFlywheel;
    }

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
}
