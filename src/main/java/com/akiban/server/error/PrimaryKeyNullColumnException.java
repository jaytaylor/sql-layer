
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public class PrimaryKeyNullColumnException extends InvalidOperationException {
    
    public PrimaryKeyNullColumnException (TableName name, String columnName) {
        super(ErrorCode.PK_NULL_COLUMN, name.getSchemaName(), name.getTableName(), columnName);
    }
}
