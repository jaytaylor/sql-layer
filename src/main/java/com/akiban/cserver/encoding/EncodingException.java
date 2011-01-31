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

package com.akiban.cserver.encoding;

public final class EncodingException extends RuntimeException {
    EncodingException(String message, Throwable e) {
        super(message, e);
    }
    EncodingException(String message) {
        super(message);
    }

    public static EncodingException dueTo(Throwable cause) {
        if (cause instanceof EncodingException) {
            return (EncodingException)cause;
        }
        return new EncodingException(cause);
    }

    private EncodingException(Throwable cause) {
        super(cause);
    }
}
