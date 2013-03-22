
package com.akiban.server.error;

public final class NoSuchIndexException extends InvalidOperationException {
    public NoSuchIndexException(String indexName) {
        super(ErrorCode.NO_INDEX, indexName);
    }
}
