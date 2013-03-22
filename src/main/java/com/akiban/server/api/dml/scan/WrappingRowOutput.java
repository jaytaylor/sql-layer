
package com.akiban.server.api.dml.scan;

import com.akiban.server.rowdata.RowData;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.GrowableByteBuffer;

public class WrappingRowOutput implements LegacyRowOutput {
    private int markPos = -1;
    protected final GrowableByteBuffer wrapped;
    private int rows;

    /**
     * Creates a RowOutput that returns the given ByteBuffer
     * @param buffer the ByteBuffer to wrap
     * @throws IllegalArgumentException is buffer if null
     */
    public WrappingRowOutput(GrowableByteBuffer buffer) {
        ArgumentValidation.notNull("buffer", buffer);
        this.wrapped = buffer;
    }

    @Override
    final public GrowableByteBuffer getOutputBuffer() {
        return wrapped;
    }

    @Override
    final public void wroteRow(boolean limitExceeded) {
        if (!limitExceeded) {
            ++rows;
            postWroteRow();
        }
    }

    @Override
    public void addRow(RowData rowData)
    {
        throw new UnsupportedOperationException("Shouldn't be using addRow for output to a ScanRowsResponse message");
    }

    protected void postWroteRow() {

    }

    @Override
    final public int getRowsCount() {
        return rows;
    }

    @Override
    public boolean getOutputToMessage()
    {
        return true;
    }

    @Override
    public void mark() {
        markPos = wrapped.position();
    }

    @Override
    public void rewind() {
        wrapped.position(markPos);
    }
}
