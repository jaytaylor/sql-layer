
package com.akiban.server.error;

public class UnsupportedExplainException extends InvalidOperationException {
    public UnsupportedExplainException() {
        super(ErrorCode.UNSUPPORTED_EXPLAIN);
    }
}
