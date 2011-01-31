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

package com.akiban.cserver.api.dml.scan;

import java.util.EnumSet;
import java.util.Set;

import com.akiban.cserver.api.common.TableId;

public class ScanAllRequest extends ScanAllRange implements ScanRequest {

    private static final int SCAN_FLAGS = ScanFlag.toRowDataFormat(EnumSet.noneOf(ScanFlag.class));

    public ScanAllRequest(TableId tableId, Set<Integer> columnIds) {
        super(tableId, columnIds);
    }

    @Override
    public int getIndexId() {
        return 0;
    }

    @Override
    public int getScanFlags() {
        return SCAN_FLAGS;
    }
}
