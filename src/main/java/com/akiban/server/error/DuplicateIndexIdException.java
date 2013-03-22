
package com.akiban.server.error;

import com.akiban.ais.model.IndexName;

public class DuplicateIndexIdException extends InvalidOperationException {
    public DuplicateIndexIdException (IndexName name1, IndexName name2) {
        super (ErrorCode.DUPLICATE_INDEXID, 
                name1.getSchemaName(), name1.getTableName(), name1.getName(),
                name2.getSchemaName(), name2.getTableName(), name2.getName());
    }
}
