
package com.akiban.server.api.dml.scan;

import com.akiban.server.rowdata.RowData;
import com.akiban.server.api.dml.ColumnSelector;

import java.util.Arrays;

public class LegacyScanRequest extends LegacyScanRange implements ScanRequest {
    private final int indexId;
    private final int scanFlags;
    private ScanLimit limit;

    @Override
    public int getIndexId() {
        return indexId;
    }

    @Override
    public int getScanFlags() {
        return scanFlags;
    }

    public LegacyScanRequest(int tableId,
                             RowData start,
                             ColumnSelector startColumns,
                             RowData end,
                             ColumnSelector endColumns,
                             byte[] columnBitMap,
                             int indexId,
                             int scanFlags,
                             ScanLimit limit)
    {
        super(tableId, start, startColumns, end, endColumns, columnBitMap);
        this.indexId = indexId;
        this.scanFlags = scanFlags;
        this.limit = limit;
    }

    @Override
    public String toString() {
        return String.format("Scan[ tableId=%d, indexId=%d, scanFlags=0x%02X, projection=%s start=<%s> end=<%s>",
                tableId, indexId, scanFlags, Arrays.toString(columnBitMap), start, end
        );
    }

    @Override
    public ScanLimit getScanLimit() {
        return limit;
    }

    @Override
    public void dropScanLimit()
    {
        limit = ScanLimit.NONE;
    }
}
