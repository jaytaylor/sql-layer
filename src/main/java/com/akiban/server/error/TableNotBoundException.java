package com.akiban.server.error;

public class TableNotBoundException extends InvalidOperationException {
    public TableNotBoundException (String statement) {
        super (ErrorCode.TABLE_NOT_BOUND, statement);
    }
}
