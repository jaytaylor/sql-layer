
package com.akiban.server.error;

public class UnsupportedGroupIndexJoinTypeException extends
        InvalidOperationException {
    public UnsupportedGroupIndexJoinTypeException(String indexName) {
        super(ErrorCode.UNSUPPORTED_GROUP_INDEX_JOIN, indexName);
    }
}
