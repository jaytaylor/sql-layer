package com.akiban.server.error;

import com.akiban.ais.model.IndexName;
import com.akiban.ais.model.TableName;

public class DuplicateIndexException extends InvalidOperationException {
    
    public DuplicateIndexException (IndexName name) {
        super(ErrorCode.DUPLICATE_INDEXES, name.getSchemaName(), name.getTableName(), name.getName());
    }
    public DuplicateIndexException (TableName table, String indexName) {
        super(ErrorCode.DUPLICATE_INDEXES, table.getSchemaName(), table.getTableName(), indexName);
    }
}
