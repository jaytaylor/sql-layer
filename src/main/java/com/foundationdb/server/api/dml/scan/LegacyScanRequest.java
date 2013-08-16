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
import com.foundationdb.server.api.dml.ColumnSelector;

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
