package com.akiban.cserver.api.dml.scan;

import com.akiban.cserver.api.common.ByteBufferWriter;
import com.akiban.cserver.api.common.WrongByteAllocationException;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public final class CursorId extends ByteBufferWriter {

    private final long cursorId;

    public CursorId(long id) {
        this.cursorId = id;
    }

    /**
     * Reads an int from the buffer.
     * @param readFrom the buffer to read from
     * @param allocatedBytes must be 4
     * @throws java.nio.BufferUnderflowException if thrown from reading the buffer
     */
    public CursorId(ByteBuffer readFrom, int allocatedBytes) {
        WrongByteAllocationException.ifNotEqual(allocatedBytes, Long.SIZE/8);
        cursorId = readFrom.getInt();
    }

    @Override
    protected void writeToBuffer(ByteBuffer output) throws Exception {
        output.putLong(cursorId);
    }

    public long getCursorId() {
        return cursorId;
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
}
