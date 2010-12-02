package com.akiban.cserver.api.dml.scan;

import com.akiban.cserver.store.RowCollector;

public final class Cursor {

    private CursorState state = CursorState.FRESH;
    private final RowCollector rowCollector;

    public Cursor(RowCollector rowCollector) {
        this.rowCollector = rowCollector;
    }

    public RowCollector getRowCollector() {
        return rowCollector;
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

    public CursorState getState() {
        return state;
    }
}
