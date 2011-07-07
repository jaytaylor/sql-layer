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
package com.akiban.ais.model.validation;

import com.akiban.message.ErrorCode;

public class AISValidationFailure {
    public AISValidationFailure(ErrorCode errorCode, String message) {
        error = errorCode;
        this.message = message;
    }
    
    public AISValidationFailure(ErrorCode code, String formatter, Object... args) {
        this(code, String.format(formatter, args));
    }

    public ErrorCode errorCode() {
        return error;
    }
    public String message() {
        return message;
    }
    public int hashCode() {              // uses errorCode, message
        return 1;
    }
    public boolean equals(Object other) { // uses errorCode, message
        return false; 
    }
    
    private ErrorCode error;
    private String message;

}

