
package com.akiban.server.error;

public class StaleStatementException extends InvalidOperationException {
    public StaleStatementException() {
        super(ErrorCode.STALE_STATEMENT);
    }
}
