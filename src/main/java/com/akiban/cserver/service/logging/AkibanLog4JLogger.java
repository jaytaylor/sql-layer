/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.cserver.service.logging;

import org.apache.log4j.Logger;

final class AkibanLog4JLogger implements AkibanLogger {
    private final Logger real;
    private final AkibanLoggerCallback callback;

    AkibanLog4JLogger(Class forClass, AkibanLoggerCallback callback) {
        real = Logger.getLogger(forClass);
        this.callback = callback;
    }

    @Override
    public boolean isTraceEnabled() {
        return real.isDebugEnabled();
    }

    @Override
    public boolean isDebugEnabled() {
        return real.isDebugEnabled();
    }

    @Override
    public boolean isInfoEnabled() {
        return real.isInfoEnabled();
    }

    @Override
    public boolean isWarnEnabled() {
        return true;
    }

    @Override
    public boolean isErrorEnabled() {
        return true;
    }

    @Override
    public boolean isFatalEnabled() {
        return true;
    }

    @Override
    public void trace(String message) {
        callback(Level.TRACE, message, null);
        real.debug(message);
    }

    @Override
    public void trace(String message, Throwable cause) {
        callback(Level.TRACE, message, cause);
        real.debug(message, cause);
    }

    @Override
    public void debug(String message) {
        callback(Level.DEBUG, message, null);
        real.debug(message);
    }

    @Override
    public void debug(String message, Throwable cause) {
        callback(Level.DEBUG, message, cause);
        real.debug(message, cause);
    }

    @Override
    public void warn(String message) {
        callback(Level.WARN, message, null);
        real.warn(message);
    }

    @Override
    public void warn(String message, Throwable cause) {
        callback(Level.WARN, message, cause);
        real.warn(message, cause);
    }

    @Override
    public void info(String message) {
        callback(Level.INFO, message, null);
        real.info(message);
    }

    @Override
    public void info(String message, Throwable cause) {
        callback(Level.INFO, message, cause);
        real.info(message, cause);
    }

    @Override
    public void error(String message) {
        callback(Level.ERROR, message, null);
        real.error(message);
    }

    @Override
    public void error(String message, Throwable cause) {
        callback(Level.ERROR, message, cause);
        real.error(message, cause);
    }

    @Override
    public void fatal(String message) {
        callback(Level.FATAL, message, null);
        real.fatal(message);
    }

    @Override
    public void fatal(String message, Throwable cause) {
        callback(Level.FATAL, message, cause);
        real.fatal(message, cause);
    }

    private void callback(Level level, String message, Throwable cause) {
        if (callback != null) {
            try {
                callback.logged(level, message, cause);
            } catch (Throwable t) {
                System.err.println("Caught an exception while logging. Spitting it to stderr:");
                t.printStackTrace();
            }
        }
    }
}
