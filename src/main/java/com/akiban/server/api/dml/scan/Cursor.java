/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.api.dml.scan;

import com.akiban.server.store.RowCollector;

public final class Cursor {

    private CursorState state = CursorState.FRESH;
    private final RowCollector rowCollector;
    private final ScanLimit limit;
    private final ScanRequest scanRequest;

    public Cursor(RowCollector rowCollector, ScanLimit limit, ScanRequest scanRequest) {
        this.rowCollector = rowCollector;
        this.limit = limit;
        this.scanRequest = scanRequest;
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

    public CursorState getState() {
        return state;
    }

    public boolean isClosed() {
        return ! state.isOpenState();
    }
}
