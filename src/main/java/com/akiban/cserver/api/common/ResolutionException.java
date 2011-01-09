package com.akiban.cserver.api.common;


public final class ResolutionException extends RuntimeException {
    public ResolutionException() {
        super();
    }

    public ResolutionException(Object message) {
        super(message == null ? null : message.toString());
    }
}
