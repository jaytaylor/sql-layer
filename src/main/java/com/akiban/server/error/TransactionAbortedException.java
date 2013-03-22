
package com.akiban.server.error;

public class TransactionAbortedException extends InvalidOperationException {
    public TransactionAbortedException() {
        super (ErrorCode.TRANSACTION_ABORTED);
    }
}
