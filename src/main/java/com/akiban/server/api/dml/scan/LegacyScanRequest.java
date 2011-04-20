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
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.api.dml.TableDefinitionMismatchException;

import java.util.Arrays;

public class LegacyScanRequest extends LegacyScanRange implements ScanRequest {
    private final int indexId;
    private final int scanFlags;
    private final ScanLimit limit;

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
    throws TableDefinitionMismatchException
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
}
