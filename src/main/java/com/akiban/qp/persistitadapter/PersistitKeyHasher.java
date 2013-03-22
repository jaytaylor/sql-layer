
package com.akiban.qp.persistitadapter;

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
