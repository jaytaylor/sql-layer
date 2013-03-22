
package com.akiban.server.error;

import com.akiban.ais.model.IndexName;

public class DuplicateIndexTreeNamesException extends InvalidOperationException {
    public DuplicateIndexTreeNamesException (IndexName index, IndexName index2, String treeName) {
        super (ErrorCode.DUPLICATE_INDEX_TREENAME,
                index.getSchemaName(), index.getTableName(), index.getName(), 
                index2.getSchemaName(), index2.getTableName(), index2.getName(),
                treeName);
    }

}
