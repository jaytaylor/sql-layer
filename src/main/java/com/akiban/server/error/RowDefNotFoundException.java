
package com.akiban.server.error;

public final class RowDefNotFoundException extends InvalidOperationException {
    private final int rowDefId;
    public RowDefNotFoundException(int rowDefId) {
        super(ErrorCode.NO_SUCH_TABLEDEF, rowDefId);
        this.rowDefId = rowDefId;
    }

    public int getId() {
        return rowDefId;
    }
}
