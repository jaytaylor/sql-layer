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

package com.akiban.cserver.api;

public final class HapiRequestException extends  Exception {
    public enum ReasonCode {
        UNKNOWN(-1),
        UNPARSABLE(0),
        UNKNOWN_IDENTIFIER(1),
        MULTIBRANCH(2),
        UNSUPPORTED_REQUEST(3),
        EXCEPTION_THROWN(4),
        WRITE_ERROR(5),
        INTERNAL_ERROR(6)
        ;

        private final int code;

        ReasonCode(int code) {
            this.code = code;
        }

        public boolean warrantsErrorLogging() {
            return this.equals(INTERNAL_ERROR) || this.equals(EXCEPTION_THROWN);
        }
    }

    private final ReasonCode reasonCode;
    private final String message;

    public HapiRequestException(String message, ReasonCode reasonCode) {
        super(message);
        this.reasonCode = reasonCode == null ? ReasonCode.UNKNOWN : reasonCode;
        this.message = message;
    }

    public HapiRequestException(String message, Exception cause) {
        super(message, cause);
        this.reasonCode = ReasonCode.EXCEPTION_THROWN;
        this.message = message;
    }

    public HapiRequestException(String message, Throwable cause, ReasonCode reasonCode) {
        super(message, cause);
        this.reasonCode = reasonCode == null ? ReasonCode.UNKNOWN : reasonCode;
        this.message = message;
    }

    @Override
    public String getMessage() {
        return "<" + reasonCode + ">: " + super.getMessage();
    }

    public String getSimpleMessage() {
        return message;
    }

    public ReasonCode getReasonCode() {
        return reasonCode;
    }
}
