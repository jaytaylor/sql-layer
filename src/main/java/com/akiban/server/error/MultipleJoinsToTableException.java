
package com.akiban.server.error;

public class MultipleJoinsToTableException extends InvalidOperationException {
    public MultipleJoinsToTableException (String schemaName, String tableName) {
        super (ErrorCode.MULTIPLE_JOINS, schemaName,tableName);
    }
}
