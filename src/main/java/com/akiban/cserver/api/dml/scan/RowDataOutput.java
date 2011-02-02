package com.akiban.cserver.api.dml.scan;

import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.RowData;
import com.akiban.cserver.api.DMLFunctions;
import com.akiban.cserver.service.session.Session;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class RowDataOutput implements LegacyRowOutput {
    private final ByteBuffer buffer;
    private final List<RowData> rowDatas = new ArrayList<RowData>();
    private int start;

    public RowDataOutput(ByteBuffer buffer) {
        if(!buffer.hasArray()) {
            throw new IllegalArgumentException("buffer doesn't have array");
        }
        this.buffer = buffer;
        start = buffer.position();
    }

    public List<RowData> getRowDatas() {
        return rowDatas;
    }

    @Override
    public ByteBuffer getOutputBuffer() throws RowOutputException {
        return buffer;
    }

    @Override
    public void wroteRow() throws RowOutputException {
        RowData rowData = new RowData(
                buffer.array(),
                start,
                buffer.position() - start
        );
        rowData.prepareRow(start);
        rowDatas.add(rowData);
        start = buffer.position();
    }

    @Override
    public int getRowsCount() {
        return rowDatas.size();
    }

    /**
     * Convenience method for doing a full scan (that is, a scan until there are no more rows to be scanned for the
     * request) and returning the rows.
     *
     *
     * @param session the session in which to scan
     * @param dml the DMLFunctions to handle the scan
     * @param request the scan request
     * @return the resulting rows
     * @throws InvalidOperationException if thrown at any point during the scan
     */
    public static List<RowData> scanFull(Session session, DMLFunctions dml, ByteBuffer buffer, ScanRequest request)
            throws InvalidOperationException
    {
        final RowDataOutput output = new RowDataOutput(buffer);
        CursorId scanCursor = dml.openCursor(session, request);
        try {
            while(dml.scanSome(session, scanCursor, output, -1))
            {}
            return output.getRowDatas();
        } finally {
            dml.closeCursor(session, scanCursor);
        }
    }
}
