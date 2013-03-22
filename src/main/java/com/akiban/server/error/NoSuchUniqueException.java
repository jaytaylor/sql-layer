
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public final class NoSuchUniqueException extends InvalidOperationException {
    public NoSuchUniqueException(TableName tableName, String indexName) {
        super(ErrorCode.NO_SUCH_UNIQUE, tableName.getSchemaName(), tableName.getTableName(), indexName);
    }
}
