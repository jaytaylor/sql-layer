package com.akiban.cserver.service.logging;

public interface LoggingService {
    AkibanLogger getLogger(Class<?> forClass);
}
