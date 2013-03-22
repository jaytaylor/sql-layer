
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

import com.akiban.sql.parser.QueryTreeNode;

public final class NoSuchTableException extends BaseSQLException {
    public NoSuchTableException(TableName tableName) {
        this(tableName.getSchemaName(), tableName.getTableName());
    }

    public NoSuchTableException(String schemaName, String tableName) {
        this(schemaName, tableName, null);
    }

    public NoSuchTableException(String schemaName, String tableName, QueryTreeNode referenceNode) {
        super(ErrorCode.NO_SUCH_TABLE, schemaName, tableName, referenceNode);
    }
}
