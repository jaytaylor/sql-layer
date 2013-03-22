
package com.akiban.server.error;

import com.akiban.ais.model.IndexName;

public class InvalidIndexIDException extends InvalidOperationException {
    public InvalidIndexIDException(IndexName indexName, int indexID) {
        super(ErrorCode.INVALID_INDEX_ID,
              indexName.getSchemaName(),
              indexName.getTableName(),
              indexName.getName(),
              indexID
        );
    }
}
