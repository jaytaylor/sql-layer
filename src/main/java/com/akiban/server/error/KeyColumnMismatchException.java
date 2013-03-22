package com.akiban.server.error;

public class KeyColumnMismatchException extends InvalidOperationException {
    public KeyColumnMismatchException(String reason) {
        super (ErrorCode.KEY_COLUMN_MISMATCH, reason);
    }

}
