package com.akiban.cserver.encoding;

public final class EncodingException extends RuntimeException {
    EncodingException(String message, Throwable e) {
        super(message, e);
    }
    EncodingException(String message) {
        super(message);
    }
}
