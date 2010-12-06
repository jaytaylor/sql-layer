package com.akiban.cserver.service.logging;

import com.akiban.cserver.service.Service;
import com.akiban.cserver.service.jmx.JmxManageable;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class LoggingServiceImpl implements LoggingService, JmxManageable, Service<LoggingService> {
    private final List<LoggingError> internalErrors = Collections.synchronizedList( new ArrayList<LoggingError>() );
    private final AtomicInteger traceCount = new AtomicInteger();
    private final AtomicInteger debugCount = new AtomicInteger();
    private final AtomicInteger warnCount = new AtomicInteger();
    private final AtomicInteger infoCount = new AtomicInteger();
    private final AtomicInteger errorCount = new AtomicInteger();
    private final AtomicInteger fatalCount = new AtomicInteger();

    private final Object INTERNAL_LOCK = new Object();

    private final AkibanLoggerCallback callback = new AkibanLoggerCallback() {
        @Override
        public void logged(AkibanLogger.Level level, String message, Throwable cause) {
            if (level == null) {
                internalError("null level provided");
                return;
            }

            switch (level) {
                case TRACE:
                    traceCount.incrementAndGet();
                    break;
                case DEBUG:
                    debugCount.incrementAndGet();
                    break;
                case INFO:
                    infoCount.incrementAndGet();
                    break;
                case WARN:
                    warnCount.incrementAndGet();
                    break;
                case ERROR:
                    errorCount.incrementAndGet();
                    break;
                case FATAL:
                    fatalCount.incrementAndGet();
                    break;
                default:
                    internalError("Unrecognized level: " + level.name());
            }
        }
    };

    private final LoggingServiceMXBean mxBean = new LoggingServiceMXBean() {
        @Override
        public int getTraceCount() {
            return traceCount.get();
        }

        @Override
        public int getDebugCount() {
            return debugCount.get();
        }

        @Override
        public int getWarnCount() {
            return warnCount.get();
        }

        @Override
        public int getInfoCount() {
            return infoCount.get();
        }

        @Override
        public int getErrorCount() {
            return errorCount.get();
        }

        @Override
        public int getFatalCount() {
            return fatalCount.get();
        }

        @Override
        public List<LoggingError> getInternalErrors() {
            return Collections.unmodifiableList(internalErrors);
        }

        @Override
        public void clearInternalErrors() {
            internalErrors.clear();
        }

        @Override
        public void manualImput(String level, String message) {
            level = level.toUpperCase();
            try {
                final AkibanLogger.Level whatLevel = AkibanLogger.Level.valueOf(level.toUpperCase());
                callback.logged(whatLevel, message, null);
            }
            catch (IllegalArgumentException e) {
                internalError(String.format("Illegal level %s. Available levels: %s",
                        level, Arrays.asList(AkibanLogger.Level.values())));
                throw e;
            }
        }
    };

    private void internalError(String message) {
        internalErrors.add(new LoggingError(message) );
    }

    private final WeakHashMap<Class<?>, AkibanLogger> loggers = new WeakHashMap<Class<?>, AkibanLogger>();

    @Override
    public JmxObjectInfo getJmxObjectInfo() {
        return new JmxObjectInfo("Logging", mxBean, LoggingServiceMXBean.class);
    }

    @Override
    public AkibanLogger getLogger(Class<?> forClass) {
        AkibanLogger logger;
        synchronized (INTERNAL_LOCK) {
            logger = loggers.get(forClass);
            if (logger == null) {
                logger = new AkibanLog4JLogger(forClass, callback);
                loggers.put(forClass, logger);
            }
        }
        return logger;
    }

    @Override
    public void start() throws Exception {
        // no-op
    }

    @Override
    public void stop() {
        // no-op
    }

    @Override
    public LoggingService cast() {
        return this;
    }

    @Override
    public Class<LoggingService> castClass() {
        return LoggingService.class;
    }
}
