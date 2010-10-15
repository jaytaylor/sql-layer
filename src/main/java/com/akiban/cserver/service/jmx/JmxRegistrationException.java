package com.akiban.cserver.service.jmx;

import javax.management.MalformedObjectNameException;

public final class JmxRegistrationException extends RuntimeException {
    public JmxRegistrationException(String message) {
        super(message);
    }

    public JmxRegistrationException(Class<?> registeringClass, String name) {
        super(String.format("Illegal name from %s: %s", registeringClass, name));
    }

    public JmxRegistrationException(String message, Exception cause) {
        super(message, cause);
    }

    public JmxRegistrationException(Exception cause) {
        super(cause);
    }
}
