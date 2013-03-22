
package com.akiban.sql.server;

import java.util.concurrent.atomic.AtomicInteger;

public class CacheCounters {
    private final AtomicInteger hits = new AtomicInteger(0);
    private final AtomicInteger misses = new AtomicInteger(0);

    public void incrementHits() {
        hits.incrementAndGet();
    }

    public void incrementMisses() {
        misses.incrementAndGet();
    }

    public int getHits() {
        return hits.get();
    }

    public int getMisses() {
        return misses.get();
    }

    public void reset() {
        hits.set(0);
        misses.set(0);
    }
}
