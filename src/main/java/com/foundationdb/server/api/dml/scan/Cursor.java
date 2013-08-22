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

package com.foundationdb.server.api.dml.scan;

import com.foundationdb.server.store.RowCollector;

public final class Cursor {

    private CursorState state = CursorState.FRESH;
    private final RowCollector rowCollector;
    private final ScanLimit limit;
    private final ScanRequest scanRequest;
    private final long createdTimestamp;

    public Cursor(RowCollector rowCollector, ScanLimit limit, ScanRequest scanRequest) {
        this.rowCollector = rowCollector;
        this.limit = limit;
        this.scanRequest = scanRequest;
        this.createdTimestamp = System.currentTimeMillis();
    }

    public RowCollector getRowCollector() {
        return rowCollector;
    }

    public ScanLimit getLimit() {
        return limit;
    }

    public ScanRequest getScanRequest() {
        return scanRequest;
    }

    public void setScanning() {
        if (CursorState.FINISHED.equals(state)) {
            throw new IllegalStateException("Can't set state to SCANNING from FINISHED");
        }
        state = CursorState.SCANNING;
    }

    public void setFinished() {
        state = CursorState.FINISHED;
    }

    public boolean isScanning() {
        return state.equals(CursorState.SCANNING);
    }

    public boolean isFinished() {
        return state.equals(CursorState.FINISHED);
    }

    public void setScanModified() {
        state = CursorState.CONCURRENT_MODIFICATION;
    }

    public void setDDLModified() {
        state = CursorState.DDL_MODIFICATION;
    }

    public CursorState getState() {
        return state;
    }

    public boolean isClosed() {
        return ! state.isOpenState();
    }

    @Override
    public String toString() {
        return String.format("Cursor(created=%d, state=%s, request=%s)", createdTimestamp, state, scanRequest);
    }
}
