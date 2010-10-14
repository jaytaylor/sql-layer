package com.akiban.cserver;

import com.akiban.message.ErrorCode;

public class InvalidOperationException extends Exception {
    private final ErrorCode code;

    public InvalidOperationException(ErrorCode code, String message) {
        super(String.format("%s: %s", code.name(), message));
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
