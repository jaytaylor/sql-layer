
package com.akiban.server.error;

import com.akiban.qp.exec.UpdateResult;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;

public class TooManyRowsUpdatedException extends InvalidOperationException {
    public TooManyRowsUpdatedException (RowData rowData, RowDef rowDef, UpdateResult result) {
        super (ErrorCode.TOO_MANY_ROWS_UPDATED, 
                result.rowsTouched(), 
                result.rowsModified(), 
                rowData.toString(rowDef));
    }
}
