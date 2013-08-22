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

package com.foundationdb.sql.server;

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
