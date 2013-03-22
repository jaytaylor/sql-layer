
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public final class JoinToUnknownTableException extends InvalidOperationException {
    public JoinToUnknownTableException () {
        super (ErrorCode.JOIN_TO_UNKNOWN_TABLE);
    }
    public JoinToUnknownTableException(TableName childTable, TableName parentTable) {
        super(ErrorCode.JOIN_TO_UNKNOWN_TABLE, 
                childTable.getSchemaName(), childTable.getTableName(),
                parentTable.getSchemaName(), parentTable.getTableName());
    }
    
}

