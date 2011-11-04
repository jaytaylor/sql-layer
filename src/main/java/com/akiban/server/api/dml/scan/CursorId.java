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

import java.nio.ByteBuffer;

import com.akiban.server.api.common.ByteBufferWriter;
import com.akiban.server.api.common.WrongByteAllocationException;
import com.akiban.util.ArgumentValidation;

public final class CursorId extends ByteBufferWriter {
    private static final int SIZE_ON_BUFFER = 2 * (Long.SIZE / 8) + Integer.SIZE / 8;

    private final long sessionId;
    private final long cursorId;
    private final int tableId;

    public CursorId(long sessionId, long cursorId, int tableId) {
        this.sessionId = sessionId;
        this.cursorId = cursorId;
        this.tableId = tableId;
    }

    /**
     * Reads an int from the buffer.
     * @param readFrom the buffer to read from
     * @param allocatedBytes must be 4
     * @throws java.nio.BufferUnderflowException if thrown from reading the buffer
     */
    public CursorId(ByteBuffer readFrom, int allocatedBytes) {
        WrongByteAllocationException.ifNotEqual(allocatedBytes, SIZE_ON_BUFFER);
        sessionId = readFrom.getLong();
        cursorId = readFrom.getLong();
        tableId = readFrom.getInt();
    }

    @Override
    protected void writeToBuffer(ByteBuffer output) throws Exception {
        output.putLong(sessionId);
        output.putLong(cursorId);
        output.putInt(tableId);
    }

    public int getTableId() {
        return tableId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CursorId that = (CursorId) o;
        return
            this.sessionId == that.sessionId &&
            this.cursorId == that.cursorId &&
            this.tableId == that.tableId;
    }

    @Override
    public int hashCode() {
        return (int) ((tableId ^ cursorId) + sessionId);
    }

    @Override
    public String toString() {
        return String.format("CursorId[session %d, table %d, cursor %d]", sessionId, tableId, cursorId);
    }
}
