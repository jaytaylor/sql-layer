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

import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.memoryadapter.MemoryTableFactory;
import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.server.service.session.Session;

public interface TableStatusCache {
    /**
     * Create a new TableStatus that will later be attached to the given
     * tableID. It will not usable until {@link TableStatus#setRowDef(RowDef)}
     * is called.
     * @param tableID ID of the table.
     * @return Associated TableStatus.
     */
    TableStatus createTableStatus(int tableID);

    /**
     * Retrieve, or create, a new table status for a memory table that will be
     * serviced by the given factory. Unlike statuses returned from the
     * {@link #createTableStatus(int)} method, these are saved by the TableStatusCache.
     * @param tableID ID of the table.
     * @param factory Factory providing rowCount.
     * @return Associated TableStatus;
     */
    TableStatus getOrCreateMemoryTableStatus(int tableID, MemoryTableFactory factory);

    /**
     * Clean up any AIS associated state stored by this cache or any of its
     * TableStatuses. At a minimum, this will set the RowDef of each TableStatus
     * to <code>null</code>.
     */
    void detachAIS();

    /** Permanently remove any state associated with the given table. */
    void clearTableStatus(Session session, Table table);
}
