
package com.akiban.server.error;

public final class WrongExpressionArityException extends InvalidOperationException {
    public WrongExpressionArityException(int expected, int actual) {
        super(ErrorCode.WRONG_FUNCTION_ARITY, expected, actual);
    }
}
