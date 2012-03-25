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

import com.akiban.server.rowdata.RowData;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.service.session.Session;
import com.akiban.util.GrowableByteBuffer;
import com.akiban.util.ListUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class RowDataOutput implements LegacyRowOutput {
    private final static Logger LOG = LoggerFactory.getLogger(RowDataOutput.class);

    private final List<RowData> rowDatas = new ArrayList<RowData>();
    private int markedRows = 0;

    public List<RowData> getRowDatas() {
        return rowDatas;
    }

    @Override
    public GrowableByteBuffer getOutputBuffer() {
        return null;
    }

    @Override
    public void wroteRow(boolean limitExceeded) {
        throw new UnsupportedOperationException("Shouldn't be calling wroteRow for output to an HAPI request");
    }

    @Override
    public void addRow(RowData rowData)
    {
        rowDatas.add(rowData);
    }

    @Override
    public int getRowsCount() {
        return rowDatas.size();
    }

    @Override
    public boolean getOutputToMessage()
    {
        return false;
    }

    @Override
    public void mark() {
        markedRows = rowDatas.size();
    }

    @Override
    public void rewind() {
        ListUtils.truncate(rowDatas, markedRows);
    }

    /**
     * Convenience method for doing a full scan (that is, a scan until there are no more rows to be scanned for the
     * request) and returning the rows.
     *
     *
     *
     * @param session the session in which to scan
     * @param dml the DMLFunctions to handle the scan
     * @param request the scan request
     * @param knownAIS the AIS generation the caller knows about
     * @return the resulting rows
     * @throws InvalidOperationException if thrown at any point during the scan
     */
    public static List<RowData> scanFull(Session session, int knownAIS, DMLFunctions dml, ScanRequest request)
            throws InvalidOperationException
    {
        final RowDataOutput output = new RowDataOutput();
        CursorId scanCursor = dml.openCursor(session, knownAIS, request);
        try {
            dml.scanSome(session, scanCursor, output);
            return output.getRowDatas();
        } catch (BufferFullException e) {
            LOG.error(String.format("This is unexpected, request: %s", request), e);
            assert false : request;
            return null;
        } finally {
            dml.closeCursor(session, scanCursor);
        }
    }
}
