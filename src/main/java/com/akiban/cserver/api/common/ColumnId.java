package com.akiban.cserver.api.common;

import com.akiban.util.ArgumentValidation;

import java.nio.ByteBuffer;

public final class ColumnId extends ByteBufferWriter implements Comparable<ColumnId> {
    private final int columnPosition;

    public ColumnId(int columnPosition) {
        ArgumentValidation.isNotNegative("position", columnPosition);
        this.columnPosition = columnPosition;
    }

    /**
     * Reads an int from the buffer.
     * @param readFrom the buffer to read from
     * @param allocatedBytes must be 4
     * @throws java.nio.BufferUnderflowException if thrown from reading the buffer
     */
    public ColumnId(ByteBuffer readFrom, int allocatedBytes) {
        WrongByteAllocationException.ifNotEqual(allocatedBytes, 4);
        columnPosition = readFrom.getInt();
    }

    @Override
    protected void writeToBuffer(ByteBuffer output) throws Exception {
        output.putInt(columnPosition);
    }

    public int getPosition() {
        return columnPosition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ColumnId columnId1 = (ColumnId) o;

        return columnPosition == columnId1.columnPosition;

    }

    @Override
    public int compareTo(ColumnId o) {
        return this.columnPosition - o.columnPosition;
    }

    @Override
    public int hashCode() {
        return columnPosition;
    }

    @Override
    public String toString() {
        return String.format("ColumnId<%d>", columnPosition);
    }
}
