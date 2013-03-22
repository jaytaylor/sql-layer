
package com.akiban.server.error;

public final class BulkloadException extends InvalidOperationException {
    public BulkloadException(String message) {
        super(ErrorCode.BULKLOAD, message);
    }
}
