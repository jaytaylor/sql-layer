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

/** Base class for all metrics. */
public interface BaseMetric<T>
{
    /** Get the unique name of this metric. */
    public String getName();

    /** Is this metric enabled for (usually durable) collection? */
    public boolean isEnabled();

    /** Get the current value. */
    public T getObject();

    /** Set the current value. */
    public void setObject(T value);
}
