
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public final class DuplicateColumnNameException extends InvalidOperationException {
    public DuplicateColumnNameException (TableName name, String columnName) {
        super (ErrorCode.DUPLICATE_COLUMN, name.getSchemaName(), name.getTableName(), columnName);
    }
}
