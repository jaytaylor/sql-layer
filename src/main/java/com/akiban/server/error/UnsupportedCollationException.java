
package com.akiban.server.error;

public final class UnsupportedCollationException extends InvalidOperationException {
    public UnsupportedCollationException (String schemaName, String tableName, String columnName, String collation) {
        super(ErrorCode.UNSUPPORTED_COLLATION, schemaName, tableName, columnName, collation);
    }
}
