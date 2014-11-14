/**
 * Copyright (C) 2009-2014 FoundationDB, LLC
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

package com.foundationdb.server.store;


import com.foundationdb.ais.model.Sequence;

/**
 * Sequence storage, cache lifetime:
 * - Each sequence gets a directory, prefix used to store a single k/v pair
 *   - key: Allocated directory prefix
 *   - value: Largest value allocated (i.e. considered consumed) for the sequence
 * - Each SQL Layer keeps a local cache of pre-allocated values (class below, configurable size)
 * - When a transaction needs a value it looks in the local cache
 *   - If the cache is empty or from a future timestamp, read + write of current_value+cache_size
 *     is made on the sequence k/v
 *   - A session post-commit hook is scheduled to update the layer wide cache
 *   - Further values will come out of the session cache
 * - Note:
 *   - The cost of updating the cache is amortized across cache_size many allocations
 *   - As there is a single k/v, updating the cache is currently serial
 *   - The layer wide cache update is a post-commit hook so it is possible to lose blocks if
 *     one connection sneaks in past a previous completed one. This only leads to gaps, not
 *     duplication.
 */
class SequenceCache
{
    private final long timestamp;
    private final long maxValue;
    private long value;

    public static Object cacheKey(Sequence s) {
        return s.getStorageUniqueKey();
    }

    public static SequenceCache newEmpty() {
        return new SequenceCache(Long.MAX_VALUE, 0, 1);
    }

    public static SequenceCache newLocal(long startValue, long cacheSize) {
        return new SequenceCache(Long.MAX_VALUE, startValue, startValue + cacheSize);
    }

    public static SequenceCache newGlobal(long timestamp, SequenceCache prevLocal) {
        return new SequenceCache(timestamp, prevLocal.value, prevLocal.maxValue);
    }


    private SequenceCache(long timestamp, long startValue, long maxValue) {
        this.timestamp = timestamp;
        this.value = startValue;
        this.maxValue = maxValue;
    }

    public synchronized long nextCacheValue() {
        if (++value == maxValue) {
            // ensure the next call to nextCacheValue also fails
            --value;
            return -1;
        }
        return value;
    }

    public synchronized long getCurrentValue() {
        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return String.format("SequenceCache(@%s, %d, %d, %d)", Integer.toHexString(hashCode()), timestamp, value, maxValue);
    }
}
