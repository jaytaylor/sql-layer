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
