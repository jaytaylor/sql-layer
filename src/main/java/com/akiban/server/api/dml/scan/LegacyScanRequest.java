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
import com.akiban.server.api.dml.TableDefinitionMismatchException;

public class LegacyScanRequest extends LegacyScanRange implements ScanRequest {
    private final int indexId;
    private final int scanFlags;
    private boolean outputToMessage = true;

    @Override
    public int getIndexId() {
        return indexId;
    }

    @Override
    public int getScanFlags() {
        return scanFlags;
    }

    @Override
    public boolean getOutputToMessage()
    {
        return outputToMessage;
    }

    @Override
    public void setOutputToMessage(boolean outputToMessage)
    {
        this.outputToMessage = outputToMessage;
    }

    public LegacyScanRequest(int tableId, RowData start, RowData end, byte[] columnBitMap, int indexId, int scanFlags)
    throws TableDefinitionMismatchException
    {
        super(tableId, start, end, columnBitMap);
        this.indexId = indexId;
        this.scanFlags = scanFlags;
    }
}
