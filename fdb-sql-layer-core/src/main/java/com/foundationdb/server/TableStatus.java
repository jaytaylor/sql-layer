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

package com.foundationdb.server;

import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.server.service.session.Session;

/**
 * Structure denotes summary information about a table, including row count,
 * uniqueId and auto-increment values. In general there is one TableStatus per
 * RowDef, and each object refers to the other.
 */
public interface TableStatus {
    /** Record that a row has been deleted. */
    void rowDeleted(Session session);

    /** Record that a row has been written. */
    void rowsWritten(Session session, long count);

    /** Reset, but do not remove, the state of a table. */
    void truncate(Session session);

    /** Set the RowDef of a given table.*/
    void setRowDef(RowDef rowDef);
    void setIndex (TableIndex pkTableIndex);

    /**
     * @return Current number of rows in the associated table.
     */
    long getRowCount(Session session);

    /**
     * @return Approximate number of rows in the associated table.
     */
    long getApproximateRowCount(Session session);

    /** @return The table ID this status is for */
    int getTableID();

    void setRowCount(Session session, long rowCount);
}
