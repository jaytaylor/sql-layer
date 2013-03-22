
package com.akiban.server.error;

public final class InsertWrongCountException extends InvalidOperationException {
    public InsertWrongCountException(int ntarget, int nexpr) {
        super(ErrorCode.INSERT_WRONG_COUNT, ntarget, nexpr);
    }
}
