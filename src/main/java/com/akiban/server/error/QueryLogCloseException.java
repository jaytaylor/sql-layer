package com.akiban.server.error;

public class QueryLogCloseException extends InvalidOperationException {
    public QueryLogCloseException (String message) {
        super (ErrorCode.QUERY_LOG_CLOSE_FAIL, message);
    }
}
