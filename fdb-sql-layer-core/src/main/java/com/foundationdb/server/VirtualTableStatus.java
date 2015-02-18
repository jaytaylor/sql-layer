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

import com.foundationdb.qp.virtual.VirtualScanFactory;
import com.foundationdb.server.service.session.Session;

public class VirtualTableStatus implements TableStatus
{
    private final int tableID;
    private final VirtualScanFactory factory;

    public VirtualTableStatus(int tableID, VirtualScanFactory factory) {
        assert factory != null;
        this.tableID = tableID;
        this.factory = factory;
    }

    @Override
    public synchronized long getRowCount(Session session) {
        return factory.rowCount(session);
    }

    @Override
    public synchronized void setRowCount(Session session, long rowCount) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getApproximateRowCount(Session session) {
        return getRowCount(session);
    }

    @Override
    public int getTableID() {
        return tableID;
    }

    @Override
    public synchronized void rowDeleted(Session session) {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized void rowsWritten(Session session, long count) {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized void truncate(Session session) {
        throw new UnsupportedOperationException();
    }
}
