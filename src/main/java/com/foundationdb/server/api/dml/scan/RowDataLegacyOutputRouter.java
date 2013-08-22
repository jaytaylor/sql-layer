/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.api.dml.scan;

import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.util.GrowableByteBuffer;

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
