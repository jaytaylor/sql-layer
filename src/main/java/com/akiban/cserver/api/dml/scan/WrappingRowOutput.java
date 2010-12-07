package com.akiban.cserver.api.dml.scan;

import com.akiban.util.ArgumentValidation;

import java.nio.ByteBuffer;

public class WrappingRowOutput implements LegacyRowOutput {
    protected final ByteBuffer wrapped;
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
    final public ByteBuffer getOutputBuffer() throws RowOutputException {
        return wrapped;
    }

    @Override
    final public void wroteRow() throws RowOutputException {
        ++rows;
        postWroteRow();
    }

    protected void postWroteRow() throws RowOutputException {

    }

    @Override
    final public int getRowsCount() {
        return rows;
    }
}
