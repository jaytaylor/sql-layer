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

import com.akiban.cserver.RowData;
import com.akiban.cserver.api.common.IdResolver;
import com.akiban.cserver.api.common.NoSuchTableException;

public interface ScanRange {
    RowData getStart(IdResolver idResolver) throws NoSuchTableException;
    RowData getEnd(IdResolver idResolver) throws NoSuchTableException;
    byte[] getColumnBitMap();
    int getTableIdInt(IdResolver idResolver) throws NoSuchTableException;

    /**
     * If this is provided, {@linkplain #getColumnBitMap()} should be ignored, and the behavior should be as if the
     * column bit map were for all of the table's columns.
     * @return
     */
    boolean scanAllColumns();
}
