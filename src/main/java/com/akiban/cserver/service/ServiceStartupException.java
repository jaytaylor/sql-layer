package com.akiban.cserver.service;

public final class ServiceStartupException extends Exception {
    public ServiceStartupException(String message) {
        super(message);
    }

    public ServiceStartupException(String message, Throwable cause) {
        super(message, cause);
    }

    public ServiceStartupException(Throwable cause) {
        super(cause);
    }
}
