
package com.akiban.server.error;

public final class AuthenticationFailedException extends InvalidOperationException {
    public AuthenticationFailedException(String msg) {
        super(ErrorCode.AUTHENTICATION_FAILED, msg);
    }

    public AuthenticationFailedException(Throwable cause) {
        this(cause.toString());
        initCause(cause);
    }
}
