
package com.akiban.server.error;

public class MissingGroupIndexJoinTypeException extends
        InvalidOperationException {
    public MissingGroupIndexJoinTypeException() {
        super(ErrorCode.MISSING_GROUP_INDEX_JOIN);
    }
}
