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

package com.foundationdb.server.service.metrics;

import com.foundationdb.Transaction;
import com.foundationdb.async.Future;

import java.util.List;

/** Additional methods for metrics stored in FDB. */
public interface FDBMetric<T> extends BaseMetric<T>
{
    /** Set whether this metric is enabled for this and future sessions. */
    public void setEnabled(boolean enabled);

    /** Simple <code>struct</code> for time / value pair.
     * @see #readAllValues.
     */
    public static class Value<T> {
        final long time;
        final T value;

        public Value(long time, T value) {
            this.time = time;
            this.value = value;
        }

        @Override
        public String toString() {
            return value + " at " + time;
        }
    }

    /** Retrieve all previously stored values for this metrics. */
    public Future<List<Value<T>>> readAllValues(Transaction tr);
}
