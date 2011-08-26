package com.akiban.server.error;

public class UnsupportedUniqueGroupIndexException extends
        InvalidOperationException {
    public UnsupportedUniqueGroupIndexException (String indexName) {
        super(ErrorCode.UNSUPPORTED_GROUP_UNIQUE, indexName);
    }
}
