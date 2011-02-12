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

import java.util.EnumSet;
import java.util.Set;

public class ScanAllRequest extends ScanAllRange implements ScanRequest {
    private final int indexId;
    private final int scanFlags;
    
    public ScanAllRequest(int tableId, Set<Integer> columnIds) {
        this(tableId, columnIds, 0, null);
    }

    public ScanAllRequest(int tableId, Set<Integer> columnIds, int indexId, EnumSet<ScanFlag> scanFlags)
    {
        super(tableId, columnIds);
        this.indexId = indexId;
        this.scanFlags = ScanFlag.toRowDataFormat(scanFlags != null ? scanFlags : EnumSet.noneOf(ScanFlag.class));
    }

    @Override
    public int getIndexId() {
        return indexId;
    }

    @Override
    public int getScanFlags() {
        return scanFlags;
    }
}
