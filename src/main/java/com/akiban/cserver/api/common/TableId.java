package com.akiban.cserver.api.common;

import java.nio.ByteBuffer;

public final class TableId extends ByteBufferWriter {
    private final int tableId;

    public TableId(String schema, String table) {
        throw new UnsupportedOperationException();
    }

    public TableId(int tableId) {
        this.tableId = tableId;
    }

    /**
     * Reads an int from the buffer.
     * @param readFrom the buffer to read from
     * @param allocatedBytes must be 4
     * @throws java.nio.BufferUnderflowException if thrown from reading the buffer
     */
    public TableId(ByteBuffer readFrom, int allocatedBytes) {
        WrongByteAllocationException.ifNotEqual(allocatedBytes, 4);
        tableId = readFrom.getInt();
    }

    @Override
    protected void writeToBuffer(ByteBuffer output) throws Exception {
        output.putInt(tableId);
    }

    public int getTableId() {
        return tableId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TableId tableId1 = (TableId) o;

        return tableId == tableId1.tableId;

    }

    @Override
    public int hashCode() {
        return tableId;
    }
}
