/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.error;

import java.text.MessageFormat;

public abstract class InvalidOperationException extends RuntimeException
{
    private static final long serialVersionUID = 1L;
    
    private final ErrorCode code;
    private final String shortMessage;

    private InvalidOperationException(ErrorCode code, String message) {
        super(String.format("%s: %s", code.name(), message));
        this.code = code;
        this.shortMessage = message;
        assert this.getClass().equals(code.associatedExceptionClass());
    }
    
    InvalidOperationException(ErrorCode code, Object... args) {
        this (code, MessageFormat.format (code.getMessage(), args)); 
    }

    public ErrorCode getCode() {
        return code;
    }

    public String getShortMessage() {
        return shortMessage;
    }
}
