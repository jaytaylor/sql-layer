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

package com.foundationdb.server.service.monitor;

public interface ServerMonitor {
    /** What kind of server is this? */
    String getServerType();

    /** The port in use, or <code>-1</code> if not listening or not applicable. */
    int getLocalPort();

    /** The host in use, or <code>null</code> if not listening or not applicable. */
    String getLocalHost();

    /** The system time at which this server started. */
    long getStartTimeMillis();
    
    /** The total number of sessions services, including those no longer active. */
    int getSessionCount();
}
