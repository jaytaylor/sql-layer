package com.akiban.server.error;

public class FKValueMismatchException extends InvalidOperationException {

    public FKValueMismatchException(String columnName) {
        super(ErrorCode.FK_VALUE_MISMATCH, columnName);
    }
}
