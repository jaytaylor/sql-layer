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

package com.foundationdb.server.api;

import com.foundationdb.server.service.session.Session;

public interface DMLFunctions {

    /**
     * Truncates the given table.
     *
     * @param tableId the table to truncate
     */
    void truncateTable(Session session, int tableId);

    /**
     * Truncates the given table, possibly cascading the truncate to child tables.
     *
     * @param tableId the table to truncate
     * @param descendants <code>true</code> to delete descendants too
     */
    void truncateTable(Session session, int tableId, boolean descendants);
}
