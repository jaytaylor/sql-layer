package com.akiban.cserver.service;

public final class ServiceNotStartedException extends RuntimeException {
    public ServiceNotStartedException() {
        super();
    }

    public ServiceNotStartedException(String s) {
        super(s);
    }

    public ServiceNotStartedException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public ServiceNotStartedException(Throwable throwable) {
        super(throwable);
    }
}
