
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public class UnsupportedIndexSizeException extends InvalidOperationException {
    public UnsupportedIndexSizeException (TableName name, String indexName) {
        super (ErrorCode.UNSUPPORTED_INDEX_SIZE, name.getSchemaName(), name.getTableName(), indexName);
    }
 }
