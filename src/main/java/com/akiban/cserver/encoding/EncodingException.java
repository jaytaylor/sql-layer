package com.akiban.cserver.encoding;

public final class EncodingException extends RuntimeException {
    EncodingException(String message, Throwable e) {
        super(message, e);
    }
    EncodingException(String message) {
        super(message);
    }

    public static EncodingException dueTo(Throwable cause) {
        if (cause instanceof EncodingException) {
            return (EncodingException)cause;
        }
        return new EncodingException(cause);
    }

    private EncodingException(Throwable cause) {
        super(cause);
    }
}
