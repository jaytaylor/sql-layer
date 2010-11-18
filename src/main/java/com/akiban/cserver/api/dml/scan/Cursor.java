package com.akiban.cserver.api.dml.scan;

import com.akiban.cserver.store.RowCollector;

public final class Cursor {
    private enum State {
        FRESH, SCANNING, FINISHED
    }

    private State state = State.FRESH;
    private final RowCollector rowCollector;

    public Cursor(RowCollector rowCollector) {
        this.rowCollector = rowCollector;
    }

    public RowCollector getRowCollector() {
        return rowCollector;
    }

    public void setScanning() {
        if (State.FINISHED.equals(state)) {
            throw new IllegalStateException("Can't set state to SCANNING from FINISHED");
        }
        state = State.SCANNING;
    }

    public void setFinished() {
        state = State.FINISHED;
    }

    public boolean isScanning() {
        return state.equals(State.SCANNING);
    }

    public boolean isFinished() {
        return state.equals(State.FINISHED);
    }
}
