
package com.akiban.server.api.dml.scan;

import java.util.EnumSet;
import java.util.Set;

public class ScanAllRequest extends ScanAllRange implements ScanRequest {
    private final int indexId;
    private final int scanFlags;
    private ScanLimit limit;

    public ScanAllRequest(int tableId, Set<Integer> columnIds) {
        this(tableId, columnIds, 0, null);
    }

    public ScanAllRequest(int tableId,
                          Set<Integer> columnIds,
                          int indexId,
                          EnumSet<ScanFlag> scanFlags,
                          ScanLimit limit)
    {
        super(tableId, columnIds);
        this.indexId = indexId;
        this.scanFlags = ScanFlag.toRowDataFormat(scanFlags != null ? scanFlags : EnumSet.noneOf(ScanFlag.class));
        this.limit = limit;
    }
    public ScanAllRequest(int tableId,
                          Set<Integer> columnIds,
                          int indexId,
                          EnumSet<ScanFlag> scanFlags)
    {

        this(tableId, columnIds, indexId, scanFlags, ScanLimit.NONE);
    }

    @Override
    public int getIndexId() {
        return indexId;
    }

    @Override
    public int getScanFlags() {
        return scanFlags;
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
