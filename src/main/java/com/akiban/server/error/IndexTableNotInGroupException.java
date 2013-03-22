
package com.akiban.server.error;

public class IndexTableNotInGroupException extends InvalidOperationException {
    public IndexTableNotInGroupException(String indexName, String column, String tableName) {
        super (ErrorCode.INDEX_TABLE_NOT_IN_GROUP, indexName, column, tableName);
    }
}
