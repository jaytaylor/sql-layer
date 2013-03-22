
package com.akiban.server.api.dml.scan;

import com.akiban.server.rowdata.RowData;
import com.akiban.util.GrowableByteBuffer;

import java.util.ArrayList;
import java.util.List;

public final class RowDataLegacyOutputRouter implements LegacyRowOutput {
    public interface Handler {
        void mark();
        void rewind();
        void handleRow(RowData rowData);
    }

    private final List<Handler> handlers = new ArrayList<>();
    private int rows = 0;
    private int markedRows = rows;

    /**
     * Adds a handler to this router. This method returns the handler back to you, as a convenience for when
     * you want to add a new handler and also keep a reference to it.
     * @param handler the handler to add to this router
     * @param <T> the handler's type
     * @return the handler you passed in
     */
    public <T extends Handler> T addHandler(T handler) {
        handlers.add(handler);
        return handler;
    }

    @Override
    public GrowableByteBuffer getOutputBuffer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void wroteRow(boolean limitExceeded) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addRow(RowData rowData) {
        for(Handler handler : handlers) {
            handler.handleRow(rowData);
        }
        ++rows;
    }

    @Override
    public int getRowsCount() {
        return rows;
    }

    @Override
    public boolean getOutputToMessage() {
        return false;
    }

    @Override
    public void mark() {
        markedRows = rows;
        for(Handler handler : handlers) {
            handler.mark();
        }
    }

    @Override
    public void rewind() {
        rows = markedRows;
        for(Handler handler : handlers) {
            handler.rewind();
        }
    }
}
