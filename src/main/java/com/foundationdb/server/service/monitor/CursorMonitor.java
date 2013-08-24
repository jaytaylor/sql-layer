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

public interface CursorMonitor {
    /** The id of the session owning the cursor. */
    int getSessionId();

    /** The name of the cursor, if any. */
    String getName();    

    /** The SQL of the cursor's statement. */
    String getSQL();    

    /** The name of the corresponding prepared statement, if any. */
    String getPreparedStatementName();    

    /** The time at which the cursor was opened. */
    long getCreationTimeMillis();

    /** The number of rows returned so far. */
    int getRowCount();

}
