
package com.akiban.server.error;

public class UnableToExplainException extends InvalidOperationException {
    public UnableToExplainException () {
        super (ErrorCode.UNABLE_TO_EXPLAIN);
    }
}
