package com.akiban.server.error;

public class FullTextQueryParseException extends InvalidOperationException {
    public FullTextQueryParseException(Exception cause) {
        super(ErrorCode.FULL_TEXT_QUERY_PARSE, cause.getMessage());
        initCause(cause);
    }

}
