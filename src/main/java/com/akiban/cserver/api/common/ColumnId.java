package com.akiban.cserver.api.common;

import java.nio.ByteBuffer;

public final class ColumnId extends ByteBufferWriter {
    private final int columnId;

    public ColumnId(int columnId) {
        this.columnId = columnId;
    }

    /**
     * Reads an int from the buffer.
     * @param readFrom the buffer to read from
     * @param allocatedBytes must be 4
     * @throws java.nio.BufferUnderflowException if thrown from reading the buffer
     */
    public ColumnId(ByteBuffer readFrom, int allocatedBytes) {
        WrongByteAllocationException.ifNotEqual(allocatedBytes, 4);
        columnId = readFrom.getInt();
    }

    @Override
    protected void writeToBuffer(ByteBuffer output) throws Exception {
        output.putInt(columnId);
    }

    public int getColumnId() {
        return columnId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ColumnId columnId1 = (ColumnId) o;

        return columnId == columnId1.columnId;

    }

    @Override
    public int hashCode() {
        return columnId;
    }
}
