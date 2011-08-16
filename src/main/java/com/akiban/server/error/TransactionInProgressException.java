package com.akiban.server.error;

public class TransactionInProgressException extends InvalidOperationException {
    public TransactionInProgressException () {
        super (ErrorCode.TRANSACTION_IN_PROGRESS);
    }
}
