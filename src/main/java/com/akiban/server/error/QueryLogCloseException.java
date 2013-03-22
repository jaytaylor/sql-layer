
package com.akiban.server.error;

public class QueryLogCloseException extends InvalidOperationException {
    public QueryLogCloseException (Throwable cause) {
        super(ErrorCode.QUERY_LOG_CLOSE_FAIL, cause.getMessage());
        initCause(cause);
    }
}
