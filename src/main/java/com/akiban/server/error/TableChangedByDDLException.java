
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public class TableChangedByDDLException extends InvalidOperationException {
    public TableChangedByDDLException(TableName tableName) {
        super (ErrorCode.TABLE_CHANGED_BY_DDL, tableName.getSchemaName(), tableName.getTableName());
    }
}

