
package com.akiban.server.error;

public final class CursorIsUnknownException extends InvalidOperationException {

    public CursorIsUnknownException(int tableId) {
        super(ErrorCode.CURSOR_IS_UNKNOWN, tableId);
    }
}
