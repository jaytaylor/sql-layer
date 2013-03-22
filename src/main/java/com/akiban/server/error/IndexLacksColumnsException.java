
package com.akiban.server.error;

import com.akiban.ais.model.IndexName;
import com.akiban.ais.model.TableName;

public class IndexLacksColumnsException extends InvalidOperationException {
    public IndexLacksColumnsException(IndexName indexName) {
        this(indexName.getFullTableName(), indexName.getName());
    }

    public IndexLacksColumnsException(TableName table, String indexName) {
        super(ErrorCode.INDEX_LACKS_COLUMNS, table.getSchemaName(), table.getTableName(), indexName);
    }
}
