package com.akiban.cserver.service.logging;

public interface AkibanLoggerCallback {
    void logged(AkibanLogger.Level level, String message, Throwable cause);
}
