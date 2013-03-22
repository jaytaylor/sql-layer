
package com.akiban.server.error;

public class NoActiveCursorException extends InvalidOperationException {
    public NoActiveCursorException (int tableId) {
        super (ErrorCode.NO_ACTIVE_CURSOR, tableId);
    }
}