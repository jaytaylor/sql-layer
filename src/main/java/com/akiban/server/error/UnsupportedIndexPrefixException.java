
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public class UnsupportedIndexPrefixException extends InvalidOperationException {
    public UnsupportedIndexPrefixException (TableName table, String index) {
        super (ErrorCode.UNSUPPORTED_INDEX_PREFIX, table.getSchemaName(), table.getTableName(), index);
    }
}
