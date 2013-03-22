
package com.akiban.server.error;

public class TransactionReadOnlyException extends InvalidOperationException {
    public TransactionReadOnlyException () {
        super (ErrorCode.TRANSACTION_READ_ONLY);
    }
}
