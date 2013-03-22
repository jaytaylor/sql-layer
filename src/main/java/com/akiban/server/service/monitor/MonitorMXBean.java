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

package com.akiban.server.service.monitor;

public interface MonitorMXBean
{
    /** Enable the query log. */
    void enableQueryLog();

    /** Disable the query log. */
    void disableQueryLog();

    /** Set the filename for the query log. Must be called before
     * {@link enableQueryLog}.
     */
    void setQueryLogFileName(String fileName);

    /** Get the current query log or <code>null</code> if not set. */
    String getQueryLogFileName();

    /** Set minimum time in milliseconds for a query to be logged or
     * <code>-1</code> for no limit. 
     */
    void setExecutionTimeThreshold(long threshold);

    /** Get minimum time in milliseconds for a query to be logged or
     * <code>-1</code> for no limit. 
     */
    long getExecutionTimeThreshold();
}
