
package com.akiban.server.error;

public final class DivisionByZeroException extends InvalidOperationException {
    public DivisionByZeroException() {
        super(ErrorCode.DIVIDE_BY_ZERO);
    }
}
