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
package com.akiban.server.error;

import com.akiban.qp.exec.UpdateResult;
import com.akiban.server.RowData;
import com.akiban.server.RowDef;

public class TooManyRowsUpdatedException extends InvalidOperationException {
    public TooManyRowsUpdatedException (RowData rowData, RowDef rowDef, UpdateResult result) {
        super (ErrorCode.TOO_MANY_ROWS_UPDATED, 
                result.rowsTouched(), 
                result.rowsModified(), 
                rowData.toString(rowDef));
    }
}
