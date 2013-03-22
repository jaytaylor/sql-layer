
package com.akiban.server.error;

public class IndexColNotInGroupException extends InvalidOperationException {
    public IndexColNotInGroupException (String indexName, String colName) {
        super (ErrorCode.INDEX_COL_NOT_IN_GROUP, indexName, colName);
    }
}
