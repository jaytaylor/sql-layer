
package com.akiban.server.error;

import com.akiban.ais.model.Index;

public class IndexTreeNameIsNullException extends InvalidOperationException {
    public IndexTreeNameIsNullException(Index index) {
        super(ErrorCode.INDEX_TREE_NAME_IS_NULL,
              index.getIndexName().getSchemaName(),
              index.getIndexName().getTableName(),
              index.getIndexName().getName());
    }
}
