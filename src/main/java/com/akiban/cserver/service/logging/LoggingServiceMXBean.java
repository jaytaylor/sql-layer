package com.akiban.cserver.service.logging;

import java.util.List;

public interface LoggingServiceMXBean {
    int getTraceCount();
    int getDebugCount();
    int getWarnCount();
    int getInfoCount();
    int getErrorCount();
    int getFatalCount();

    List<LoggingError> getInternalErrors();
    void clearInternalErrors();

    void manualImput(String level, String message);
}
