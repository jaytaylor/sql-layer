package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public class JoinToProtectedTableException extends InvalidOperationException {
    public JoinToProtectedTableException (TableName name, TableName joinTo) {
        super (ErrorCode.JOIN_TO_PROTECTED_TABLE, 
                name.getSchemaName(), name.getTableName(), 
                joinTo.getSchemaName(), joinTo.getTableName());
    }
}
