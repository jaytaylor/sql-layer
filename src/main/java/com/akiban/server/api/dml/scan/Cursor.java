
package com.akiban.server.api.dml.scan;

import com.akiban.server.store.RowCollector;

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
