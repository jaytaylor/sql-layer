
package com.akiban.server.error;

public final class RowOutputException extends InvalidOperationException {
    public RowOutputException (int rowCount) {
        super(ErrorCode.ROW_OUTPUT, rowCount);
    }
}
