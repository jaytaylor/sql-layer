package com.akiban.cserver.api.dml.scan;

import com.akiban.cserver.RowData;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class RowDataOutput implements LegacyRowOutput {
    private final ByteBuffer buffer;
    private final List<RowData> rowDatas = new ArrayList<RowData>();
    private int start = 0;

    public RowDataOutput(ByteBuffer buffer) {
        if(!buffer.hasArray()) {
            throw new IllegalArgumentException("buffer doesn't have array");
        }
        this.buffer = buffer;
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
}
