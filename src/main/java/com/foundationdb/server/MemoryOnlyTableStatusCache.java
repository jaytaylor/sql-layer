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
import com.foundationdb.server.service.session.Session;

import java.util.HashMap;
import java.util.Map;

public class MemoryOnlyTableStatusCache implements TableStatusCache {
    private final Map<Integer,MemoryTableStatus> tableStatusMap = new HashMap<>();
            
    @Override
    public synchronized TableStatus createTableStatus(int tableID) {
        return new MemoryTableStatus(tableID, null);
    }

    @Override
    public synchronized TableStatus getOrCreateMemoryTableStatus(int tableID, MemoryTableFactory factory) {
        return getInternalTableStatus(tableID, factory);
    }

    @Override
    public synchronized void detachAIS() {
        for(MemoryTableStatus status : tableStatusMap.values()) {
            status.setRowDef(null);
        }
    }

    @Override
    public void clearTableStatus(Session session, Table table) {
        tableStatusMap.remove(table.getTableId());
    }

    private MemoryTableStatus getInternalTableStatus(int tableID, MemoryTableFactory factory) {
        MemoryTableStatus ts = tableStatusMap.get(tableID);
        if(ts == null) {
            ts = new MemoryTableStatus(tableID, factory);
            tableStatusMap.put(tableID, ts);
        }
        return ts;
    }
}
