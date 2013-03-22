
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public final class ColumnSizeMismatchException extends InvalidOperationException {
    public ColumnSizeMismatchException(TableName table, String columnName, String sizeType, Number expected, Number actual) {
        super(ErrorCode.COLUMN_SIZE_MISMATCH, table.getSchemaName(), table.getTableName(), columnName, sizeType, expected, actual);
    }
}
