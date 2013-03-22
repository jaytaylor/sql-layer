
package com.akiban.server.error;

public class TableIsBadSubqueryException extends InvalidOperationException {
    public TableIsBadSubqueryException (String schemaName, String tableName, String message) {
        super (ErrorCode.TABLE_BAD_SUBQUERY, schemaName, tableName, message);
    }
}
