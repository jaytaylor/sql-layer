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
