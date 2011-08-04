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

import com.akiban.server.rowdata.RowData;
import com.akiban.server.api.common.NoSuchTableException;
import com.akiban.server.api.dml.ColumnSelector;

public interface ScanRange {
    RowData getStart() throws NoSuchTableException;
    ColumnSelector getStartColumns();
    RowData getEnd() throws NoSuchTableException;
    ColumnSelector getEndColumns();
    byte[] getColumnBitMap();
    int getTableId() throws NoSuchTableException;

    /**
     * If this is provided, {@linkplain #getColumnBitMap()} should be ignored, and the behavior should be as if the
     * column bit map were for all of the table's columns.
     * @return
     */
    boolean scanAllColumns();
}
