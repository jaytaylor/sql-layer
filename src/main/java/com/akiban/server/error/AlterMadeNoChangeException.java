
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

import com.akiban.sql.parser.QueryTreeNode;

public final class AlterMadeNoChangeException extends InvalidOperationException {
    public AlterMadeNoChangeException(TableName tableName) {
        this(tableName.getSchemaName(), tableName.getTableName());
    }

    public AlterMadeNoChangeException(String schemaName, String tableName) {
        super(ErrorCode.ALTER_MADE_NO_CHANGE, schemaName, tableName);
    }
}
