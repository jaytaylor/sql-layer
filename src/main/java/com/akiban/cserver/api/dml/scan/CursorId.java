package com.akiban.cserver.api.dml.scan;

import com.akiban.cserver.api.common.ByteBufferWriter;
import com.akiban.cserver.api.common.WrongByteAllocationException;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public final class CursorId extends ByteBufferWriter {
    private static final AtomicInteger counter = new AtomicInteger();

    private final int cursorId;

    CursorId() {
        this.cursorId = counter.incrementAndGet();
    }

    /**
     * Reads an int from the buffer.
     * @param readFrom the buffer to read from
     * @param allocatedBytes must be 4
     * @throws java.nio.BufferUnderflowException if thrown from reading the buffer
     */
    public CursorId(ByteBuffer readFrom, int allocatedBytes) {
        WrongByteAllocationException.ifNotEqual(allocatedBytes, 4);
        cursorId = readFrom.getInt();
    }

    @Override
    protected void writeToBuffer(ByteBuffer output) throws Exception {
        output.putInt(cursorId);
    }

    public int getCursorId() {
        return cursorId;
    }
}
