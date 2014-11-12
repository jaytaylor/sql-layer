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

/** A collection of metrics, possibly stored someplace shared / durable. */
public interface MetricCollection
{
    /** Add a new boolean metric. */
    public BooleanMetric addBooleanMetric(String name);

    /** Add a new long metric. */
    public LongMetric addLongMetric(String name);

    /** Remove a previously added metric. */
    public <T> void removeMetric(BaseMetric<T> metric);
}
