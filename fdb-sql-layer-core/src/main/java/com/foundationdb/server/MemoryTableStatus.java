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

import com.foundationdb.qp.memoryadapter.MemoryTableFactory;
import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.server.service.session.Session;

public class MemoryTableStatus implements TableStatus
{
    private final int expectedID;
    private final MemoryTableFactory factory;
    private long autoIncrement = 0;
    private long rowCount = 0;

    public MemoryTableStatus(int expectedID) {
        this(expectedID, null);
    }

    public MemoryTableStatus(int expectedID, MemoryTableFactory factory) {
        this.expectedID = expectedID;
        this.factory = factory;
    }

    @Override
    public synchronized long getAutoIncrement(Session session) {
        return autoIncrement;
    }

    @Override
    public long getRowCount(Session session) {
        if(factory != null) {
            return factory.rowCount(session);
        }
        synchronized(this) {
            return rowCount;
        }
    }

    @Override
    public synchronized void setRowCount(Session session, long rowCount) {
        if(factory != null) {
            throw new IllegalArgumentException("Cannot set row count for memory table");
        }
        this.rowCount = rowCount;
    }

    @Override
    public long getApproximateRowCount(Session session) {
        return getRowCount(session);
    }

    @Override
    public int getTableID() {
        return expectedID;
    }

    @Override
    public synchronized void setRowDef(RowDef rowDef) {
        if((rowDef != null) && (expectedID != rowDef.getRowDefId())) {
            throw new IllegalArgumentException("RowDef ID " + rowDef.getRowDefId() +
                                               " does not match expected ID " + expectedID);
        }
    }

    @Override
    public synchronized void rowDeleted(Session session) {
        rowCount = Math.max(0, rowCount - 1);
    }

    @Override
    public synchronized void rowsWritten(Session session, long count) {
        rowCount += count;
    }

    @Override
    public synchronized void setAutoIncrement(Session session, long autoIncrement) {
        this.autoIncrement = Math.max(this.autoIncrement, autoIncrement);
    }


    @Override
    public synchronized void truncate(Session session) {
        autoIncrement = 0;
        rowCount = 0;
    }
}
