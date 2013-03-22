
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public class DuplicateGroupTreeNamesException extends InvalidOperationException {
    public DuplicateGroupTreeNamesException(TableName root1, TableName root2, String treeName) {
        super (ErrorCode.DUPLICATE_GROUP_TREENAME,
                root1.getSchemaName(), root1.getTableName(),
                root2.getSchemaName(), root2.getTableName(),
                treeName);
    }
}
