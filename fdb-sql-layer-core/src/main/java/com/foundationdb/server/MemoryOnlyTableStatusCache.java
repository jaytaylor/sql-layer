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
import com.foundationdb.qp.virtual.VirtualScanFactory;
import com.foundationdb.server.service.session.Session;

import java.util.HashMap;
import java.util.Map;

public class MemoryOnlyTableStatusCache implements TableStatusCache {
    private final Map<Integer,VirtualTableStatus> tableStatusMap = new HashMap<>();
            
    @Override
    public synchronized TableStatus createTableStatus (Table table) {
        return new VirtualTableStatus(table.getTableId(), null);
    }
    
    @Override
    public synchronized TableStatus createTableStatus(int tableID) {
        return new VirtualTableStatus(tableID, null);
    }

    @Override
    public synchronized TableStatus getOrCreateVirtualTableStatus(int tableID, VirtualScanFactory factory) {
        return getInternalTableStatus(tableID, factory);
    }

    @Override
    public synchronized void detachAIS() {
        //TODO: Nothing
    }

    @Override
    public void clearTableStatus(Session session, Table table) {
        tableStatusMap.remove(table.getTableId());
    }

    private VirtualTableStatus getInternalTableStatus(int tableID, VirtualScanFactory factory) {
        VirtualTableStatus ts = tableStatusMap.get(tableID);
        if(ts == null) {
            ts = new VirtualTableStatus(tableID, factory);
            tableStatusMap.put(tableID, ts);
        }
        return ts;
    }
}
