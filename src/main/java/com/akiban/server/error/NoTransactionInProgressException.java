
package com.akiban.server.error;

public class NoTransactionInProgressException extends InvalidOperationException {
    public NoTransactionInProgressException() {
        super (ErrorCode.NO_TRANSACTION);
    }
}
