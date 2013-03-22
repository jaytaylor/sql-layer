
package com.akiban.server.error;

public final class NotNullViolationException extends InvalidOperationException {
    public NotNullViolationException(String schemaName, String tableName, String columnName) {
        super(ErrorCode.NOT_NULL_VIOLATION, schemaName, tableName, columnName);
    }
}
