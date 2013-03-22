
package com.akiban.server.error;

import com.akiban.ais.model.Column;

public class InsertNullCheckFailedException extends InvalidOperationException {
    public InsertNullCheckFailedException(Column column) {
        super (ErrorCode.INSERT_NULL_CHECK, column.getName());
    }
}
