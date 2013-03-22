
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public class ProtectedIndexException extends InvalidOperationException {
    public ProtectedIndexException (String indexName, TableName name) {
        super(ErrorCode.PROTECTED_INDEX, indexName, name.getSchemaName(), name.getTableName());
    }
}
