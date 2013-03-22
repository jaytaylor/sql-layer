
package com.akiban.server.error;

public final class NoSuchFunctionException extends InvalidOperationException {
    public NoSuchFunctionException(String functionName) {
        super(ErrorCode.NO_SUCH_FUNCTION, functionName);
    }
}
