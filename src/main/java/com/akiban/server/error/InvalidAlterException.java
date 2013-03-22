
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public class InvalidAlterException extends InvalidOperationException {
    public InvalidAlterException(TableName tableName, String detail) {
        super(ErrorCode.INVALID_ALTER, tableName.getSchemaName(), tableName.getTableName(), detail);
    }
}
