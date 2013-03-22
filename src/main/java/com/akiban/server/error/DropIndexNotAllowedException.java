
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public class DropIndexNotAllowedException extends InvalidOperationException {
    public DropIndexNotAllowedException (String indexName, TableName name) {
        super (ErrorCode.DROP_INDEX_NOT_ALLOWED, indexName, name.getSchemaName(), name.getTableName()); 
    }

}
