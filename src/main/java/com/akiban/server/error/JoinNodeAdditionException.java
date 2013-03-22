
package com.akiban.server.error;

public class JoinNodeAdditionException extends InvalidOperationException {
    public JoinNodeAdditionException (String schemaName, String tableName, String message) {
        super (ErrorCode.JOIN_NODE_ERROR, schemaName, tableName, message);
    }
}
