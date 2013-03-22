
package com.akiban.server.error;

public class TableIndexJoinTypeException extends
        InvalidOperationException {
    public TableIndexJoinTypeException() {
        super(ErrorCode.TABLE_INDEX_JOIN);
    }
}
