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

import java.nio.ByteBuffer;

import com.akiban.cserver.api.common.ByteBufferWriter;
import com.akiban.cserver.api.common.WrongByteAllocationException;
import com.akiban.util.ArgumentValidation;

public final class CursorId extends ByteBufferWriter {
    private final int SIZE_ON_BUFFER = Long.SIZE / 8 + Integer.SIZE / 8;

    private final int tableId;
    private final long cursorId;

    public CursorId(long cursorId, int tableId) {
        ArgumentValidation.notNull("tableID", tableId);
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
        cursorId = readFrom.getLong();
        tableId = readFrom.getInt();
    }

    @Override
    protected void writeToBuffer(ByteBuffer output) throws Exception {
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

        CursorId cursorId1 = (CursorId) o;

        if (cursorId != cursorId1.cursorId) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return (int) (cursorId ^ (cursorId >>> 32));
    }

    @Override
    public String toString() {
        return String.format("CursorId[%d for tableId=%d]", cursorId, tableId);
    }
}
