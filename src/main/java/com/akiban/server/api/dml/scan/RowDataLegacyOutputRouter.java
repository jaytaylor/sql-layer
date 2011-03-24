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

package com.akiban.server.api.dml.scan;

import com.akiban.server.RowData;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public final class RowDataLegacyOutputRouter implements LegacyRowOutput {
    public interface Handler {
        void mark();
        void rewind();
        void handleRow(RowData rowData) throws RowOutputException;
    }

    private final List<Handler> handlers = new ArrayList<Handler>();
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
    public ByteBuffer getOutputBuffer() throws RowOutputException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void wroteRow(boolean limitExceeded) throws RowOutputException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addRow(RowData rowData)  throws RowOutputException {
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
