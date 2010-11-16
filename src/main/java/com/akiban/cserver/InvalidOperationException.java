package com.akiban.cserver;

import com.akiban.message.ErrorCode;

public class InvalidOperationException extends Exception
{
    private static final long serialVersionUID = 1L;
    
    private final ErrorCode code;

    public InvalidOperationException(ErrorCode code, String message) {
        super(String.format("%s: %s", code.name(), message));
        this.code = code == null
                ? ErrorCode.UNKNOWN
                : code;
    }

    public InvalidOperationException(Throwable cause) {
        this(ErrorCode.UNKNOWN, "Unexpected exception", cause);
    }

    public InvalidOperationException(ErrorCode code, String message, Throwable cause) {
        super(String.format("%s: %s", code.name(), message), cause);
        this.code = code == null
                ? ErrorCode.UNKNOWN
                : code;
    }

    public InvalidOperationException(ErrorCode code, String formatter, Object... args) {
        super(code.name() + ": " + String.format(formatter, args));
        this.code = code == null
                ? ErrorCode.UNKNOWN
                : code;
    }

    public ErrorCode getCode() {
        return code;
    }
}
