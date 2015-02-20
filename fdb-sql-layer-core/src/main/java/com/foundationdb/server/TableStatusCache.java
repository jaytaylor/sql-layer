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
import com.foundationdb.qp.virtualadapter.VirtualScanFactory;
import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.server.service.session.Session;

public interface TableStatusCache {
    /** Create a new TableStatus that will later be attached to the given tableID. */
    TableStatus createTableStatus(Table table);

    /**
     * Retrieve, or create, a new table status for a virtual table that will be
     * serviced by the given factory. These are saved by the TableStatusCache.
     */
    TableStatus getOrCreateVirtualTableStatus(int tableID, VirtualScanFactory factory);

    /** Clean up any AIS associated state stored by this cache or any of its TableStatuses. */
    void detachAIS();

    /** Permanently remove any state associated with the given table. */
    void clearTableStatus(Session session, Table table);
}
