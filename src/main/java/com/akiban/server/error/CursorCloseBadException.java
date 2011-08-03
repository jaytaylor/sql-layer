package com.akiban.server.error;

public class CursorCloseBadException extends InvalidOperationException {
    public CursorCloseBadException (int tableId) {
        super(ErrorCode.CURSOR_CLOSE_BAD, tableId);
    }
}
