
package com.akiban.server.error;

import java.text.MessageFormat;

public abstract class InvalidOperationException extends RuntimeException
{
    private static final long serialVersionUID = 1L;
    
    private final ErrorCode code;
    private final String shortMessage;

    private InvalidOperationException(ErrorCode code, String message) {
        super(String.format("%s: %s", code.name(), message));
        this.code = code;
        this.shortMessage = message;
        assert this.getClass().equals(code.associatedExceptionClass());
    }
    
    InvalidOperationException(ErrorCode code, Object... args) {
        this (code, MessageFormat.format (code.getMessage(), args)); 
    }

    public ErrorCode getCode() {
        return code;
    }

    public String getShortMessage() {
        return shortMessage;
    }
}
