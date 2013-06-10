/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.util;

import com.foundationdb.AsyncFuture;
import com.foundationdb.Database;
import com.foundationdb.FDBError;
import com.foundationdb.KeyValue;
import com.foundationdb.Nothing;
import com.foundationdb.RangeQuery;
import com.foundationdb.ReadTransaction;
import com.foundationdb.Transaction;
import com.foundationdb.tuple.Range;
import com.foundationdb.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * <p>
 *     An counter, backed by FoundationDB, which provides (probabilistically) conflict free addition or subtraction,
 *     with either transactional or snapshot reads.
 * </p>
 * <p>
 *     Heavily inspired by the 'counter' example Python layer. Uses randomly generated key suffixes to avoid
 *     write conflicts. A small percentage of counter changes trigger a short coalescing cycle, from an isolated
 *     transaction, to compact the number of outstanding keys.
 * </p>
 */
public class FDBCounter {
    private static final Logger LOG = LoggerFactory.getLogger(FDBCounter.class);
    private static final int RANDOM_KEY_SIZE = 20;
    private static final int COALESCE_LIMIT = 20;
    private static final double COALESCE_PROBABILITY = 0.1;

    private final Random random;
    private final byte[] keyPrefix;
    private final Tuple subspace;
    private final Database db;
    private final ThreadLocal<AsyncFuture<Nothing>> coalesceCommit = new ThreadLocal<>();


    public FDBCounter(Database db, byte[] keyPrefix, int seed) {
        this.keyPrefix = keyPrefix;
        this.random = new Random(seed);
        this.subspace = Tuple.from(new Object[]{ keyPrefix });
        this.db = db;
    }

    /**
     * Get the value of the counter with serializable isolation.
     * <p>
     *     Conflicts are likely if the counter is active and this transaction also performs writes.
     * </p>
     */
    public long getTransactional(Transaction tr) {
        return computeSum(tr);
    }

    /**
     * Get the value of the counter with snapshot isolation.
     */
    public long getSnapshot(Transaction tr) {
        return computeSum(tr.snapshot);
    }

    /**
     * Add the given value to the counter.
     */
    public void add(Transaction tr, long x) {
        addWithCoalesce(tr, x, true);
    }

    /**
     * Sets counter total to the given value.
     */
    public void set(Transaction tr, long x) {
        long value = getSnapshot(tr);
        add(tr, x - value);
    }

    /**
     * Clears all stored state for the counter.
     */
    public void clearState(Transaction tr) {
        tr.clear(subspace.range().begin, subspace.range().end);
    }


    //
    // Helpers
    //

    public void addWithCoalesce(Transaction tr, long x, boolean maybeCoalesce) {
        byte[] key = encodeNewKey();
        byte[] value = encodeValue(x);
        tr.set(key, value);
        if(maybeCoalesce && (random.nextDouble() < COALESCE_PROBABILITY)) {
            coalesce(COALESCE_LIMIT);
        }
    }

    private long computeSum(ReadTransaction tr) {
        long total = 0;
        Range range = subspace.range();
        for(KeyValue kv : tr.getRange(range.begin, range.end)) {
            total += decodeValue(kv);
        }
        return total;
    }

    private void coalesce(int limit) {
        Transaction tr = db.createTransaction();
        try {
            byte[] bound = encodeNewKey();
            Range range = subspace.range();

            // Go froward from begin to bound or reverse from end to bound
            RangeQuery coalesceRange;
            if(random.nextDouble() < 0.5) {
                coalesceRange = tr.snapshot.getRange(bound, range.end).limit(limit);
            } else {
                coalesceRange = tr.snapshot.getRange(range.begin, bound).limit(limit).reverse();
            }

            // Read and remove keys, add new with summed total
            int total = 0;
            for(KeyValue kv : coalesceRange) {
                total += decodeValue(kv);
                tr.get(kv.getKey()); // real read for isolation
                tr.clear(kv.getKey());
            }
            addWithCoalesce(tr, total, false);

            coalesceCommit.set(tr.commit());
        } catch(FDBError e) {
            LOG.debug("Coalescing failure", e);
        }
    }

    private byte[] encodeNewKey() {
        return Tuple.from(keyPrefix, randID()).pack();
    }

    private synchronized byte[] randID() {
        byte[] bytes = new byte[RANDOM_KEY_SIZE];
        random.nextBytes(bytes);
        return bytes;
    }


    private static byte[] encodeValue(long x) {
        return Tuple.from(x).pack();
    }

    private static long decodeValue(KeyValue kv) {
        Tuple t = Tuple.fromBytes(kv.getValue());
        return t.getLong(0);
    }
}
