
package com.akiban.server.error;

public class NoSuchTableIdException extends InvalidOperationException {
    public NoSuchTableIdException (int tableId) {
        super (ErrorCode.NO_SUCH_TABLEID, tableId);
    }
}
