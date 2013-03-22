
package com.akiban.server.error;

import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;

public class NoRowsUpdatedException extends InvalidOperationException {
    public NoRowsUpdatedException (RowData rowData, RowDef rowDef) {
        super (ErrorCode.NO_ROWS_UPDATED, rowData.toString(rowDef));
    }
}
