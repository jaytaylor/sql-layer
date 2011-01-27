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

public interface AkibanLogger {
    public enum Level {
        TRACE, DEBUG, INFO, WARN, ERROR, FATAL
    }

    boolean isTraceEnabled();
    boolean isDebugEnabled();
    boolean isWarnEnabled();
    boolean isInfoEnabled();
    boolean isErrorEnabled();
    boolean isFatalEnabled();
    
    void trace(String message);
    void trace(String message, Throwable cause);
    void debug(String message);
    void debug(String message, Throwable cause);
    void warn(String message);
    void warn(String message, Throwable cause);
    void info(String message);
    void info(String message, Throwable cause);
    void error(String message);
    void error(String message, Throwable cause);
    void fatal(String message);
    void fatal(String message, Throwable cause);
}
