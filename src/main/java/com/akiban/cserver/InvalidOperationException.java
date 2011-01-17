package com.akiban.cserver;

import com.akiban.message.ErrorCode;

public class InvalidOperationException extends Exception
{
    private static final long serialVersionUID = 1L;
    
    private final ErrorCode code;
    private final String shortMessage;

    public InvalidOperationException(ErrorCode code, String message) {
        super(String.format("%s: %s", code.name(), message));
        this.code = code == null
                ? ErrorCode.UNKNOWN
                : code;
        this.shortMessage = message;
    }

    public InvalidOperationException(Throwable cause) {
        this(ErrorCode.UNKNOWN, "Unexpected exception", cause);
    }

    public InvalidOperationException(ErrorCode code, String message, Throwable cause) {
        super(String.format("%s: %s", code.name(), message), cause);
        this.code = code == null
                ? ErrorCode.UNKNOWN
                : code;
        this.shortMessage = message;
    }

    public InvalidOperationException(ErrorCode code, String formatter, Object... args) {
        this(code, String.format(formatter, args));
    }

    public ErrorCode getCode() {
        return code;
    }

    public String getShortMessage() {
        return shortMessage;
    }
}
