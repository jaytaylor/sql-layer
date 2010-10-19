package com.akiban.cserver.service.config;

public final class PropertyNotDefinedException extends RuntimeException {
    PropertyNotDefinedException(String message) {
        super(message);
    }

    PropertyNotDefinedException(String module, String key) {
        super(module + " > " + key);
    }
}
