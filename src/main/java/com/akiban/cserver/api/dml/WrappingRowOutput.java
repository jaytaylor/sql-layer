package com.akiban.cserver.api.dml;

import com.akiban.util.ArgumentValidation;

import java.nio.ByteBuffer;

public final class WrappingRowOutput implements RowOutput {
    private final ByteBuffer wrapped;
    private int rows;

    /**
     * Creates a RowOutput that returns the given ByteBuffer
     * @param buffer the ByteBuffer to wrap
     * @throws IllegalArgumentException is buffer if null
     */
    public WrappingRowOutput(ByteBuffer buffer) {
        ArgumentValidation.notNull("buffer", buffer);
        this.wrapped = buffer;
    }

    @Override
    public ByteBuffer getOutputBuffer() throws RowOutputException {
        return wrapped;
    }

    @Override
    public void wroteRow() {
        ++rows;
    }

    @Override
    public int getRowsCount() {
        return rows;
    }
}
