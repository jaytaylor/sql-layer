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

package com.akiban.cserver;

import com.akiban.message.ErrorCode;

public class InvalidOperationException extends Exception
{
    private static final long serialVersionUID = 1L;
    
    private final ErrorCode code;
    private final String shortMessage;

    public InvalidOperationException(ErrorCode code, String message) {
        super(String.format("%s: %s", code.name(), message));
        this.code = code == null
                ? ErrorCode.UNKNOWN
                : code;
        this.shortMessage = message;
    }

    public InvalidOperationException(Throwable cause) {
        this(ErrorCode.UNKNOWN, "Unexpected exception", cause);
    }

    public InvalidOperationException(ErrorCode code, String message, Throwable cause) {
        super(String.format("%s: %s", code.name(), message), cause);
        this.code = code == null
                ? ErrorCode.UNKNOWN
                : code;
        this.shortMessage = message;
    }

    public InvalidOperationException(ErrorCode code, String formatter, Object... args) {
        this(code, String.format(formatter, args));
    }

    public ErrorCode getCode() {
        return code;
    }

    public String getShortMessage() {
        return shortMessage;
    }
}
