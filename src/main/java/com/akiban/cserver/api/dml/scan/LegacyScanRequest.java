package com.akiban.cserver.api.dml.scan;

import com.akiban.cserver.RowData;
import com.akiban.cserver.api.dml.TableDefinitionMismatchException;

public class LegacyScanRequest extends LegacyScanRange implements ScanRequest {
    private final int indexId;
    private final int scanFlags;

    @Override
    public int getIndexId() {
        return indexId;
    }

    @Override
    public int getScanFlags() {
        return scanFlags;
    }

    public LegacyScanRequest(int tableId, RowData start, RowData end, byte[] columnBitMap, int indexId, int scanFlags)
    throws TableDefinitionMismatchException
    {
        super(tableId, start, end, columnBitMap);
        this.indexId = indexId;
        this.scanFlags = scanFlags;
    }
}
