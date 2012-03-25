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

import java.nio.ByteBuffer;

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
