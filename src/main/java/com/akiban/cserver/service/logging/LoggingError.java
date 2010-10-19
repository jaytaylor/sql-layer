package com.akiban.cserver.service.logging;

import java.util.Date;

@SuppressWarnings("unused") // called via JMX
public class LoggingError {
    private final String message;
    private final long timestamp;
    private final StackTraceElement[] stack;

    LoggingError(String message) {
        this.message = message;
        this.stack = Thread.currentThread().getStackTrace();
        this.timestamp = System.currentTimeMillis();
    }

    public String getMessage() {
        return message;
    }

    public StackTraceElement[] getStackTrace() {
        final StackTraceElement[] ret = new StackTraceElement[stack.length];
        System.arraycopy(stack, 0, ret, 0, stack.length);
        return ret;
    }

    public String getTimestampString() {
        final Date date = new Date(timestamp);
        return date.toString();
    }
}
