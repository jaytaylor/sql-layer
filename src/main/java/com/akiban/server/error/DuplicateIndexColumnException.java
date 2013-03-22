
package com.akiban.server.error;

import com.akiban.ais.model.Index;

public class DuplicateIndexColumnException extends InvalidOperationException {
    public DuplicateIndexColumnException (Index index, String columnName) {
        super(ErrorCode.DUPLICATE_INDEX_COLUMN, 
                index.getIndexName().getSchemaName(), index.getIndexName().getTableName(), index.getIndexName().getName(),
                columnName);
    }
}
