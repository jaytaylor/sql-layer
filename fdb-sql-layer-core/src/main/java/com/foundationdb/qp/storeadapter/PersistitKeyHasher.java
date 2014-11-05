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

package com.foundationdb.qp.storeadapter;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.persistit.Key;

class PersistitKeyHasher
{
    // For hashing a single-segment key

    public long hash(Key key, int depth)
    {
        key.indexTo(depth);
        int startPosition = key.getIndex();
        key.indexTo(depth + 1);
        int endPosition = key.getIndex();
        return hashFunction.hashBytes(key.getEncodedBytes(), startPosition, endPosition - startPosition).asLong();
    }

    // Object state

    private final HashFunction hashFunction = Hashing.goodFastHash(64); // Because we're returning longs
}
