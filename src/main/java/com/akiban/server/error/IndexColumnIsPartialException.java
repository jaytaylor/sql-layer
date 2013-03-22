
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public class IndexColumnIsPartialException extends InvalidOperationException {
    public IndexColumnIsPartialException(TableName table, String indexName, int indexPosition) {
        super(ErrorCode.INDEX_COLUMN_IS_PARTIAL, table.getSchemaName(), table.getTableName(), indexName, indexPosition);
    }
}
