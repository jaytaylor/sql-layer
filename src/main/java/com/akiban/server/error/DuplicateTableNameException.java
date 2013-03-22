
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public final class DuplicateTableNameException extends InvalidOperationException {
    public DuplicateTableNameException (TableName name) {
        super (ErrorCode.DUPLICATE_TABLE, name.getSchemaName(), name.getTableName());
    }
    
    public DuplicateTableNameException(String schemaName, String tableName)
    {
        super(ErrorCode.DUPLICATE_TABLE, schemaName, tableName);
    }
}
