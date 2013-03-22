
package com.akiban.server.error;

public class SecurityException extends InvalidOperationException {
    public SecurityException(String message) {
        super(ErrorCode.SECURITY, message);
    }

    public SecurityException(String message, Throwable cause) {
        super(ErrorCode.SECURITY, message);
        initCause(cause);
    }
}
