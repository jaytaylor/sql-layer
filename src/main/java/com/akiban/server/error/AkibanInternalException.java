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
package com.akiban.server.error;

/**
 * Akiban Internal Exceptions are errors internal to the Akiban Server, 
 * which should never happen but might. These are Exceptions on the 
 * same order as @see NullPointerException, indicating bugs in the code. 
 * These are defined to give maximum visibility to these problems as 
 * quickly and with as much information as possible.  
 * @author tjoneslo
 *
 */
public class AkibanInternalException extends RuntimeException {
    
    public AkibanInternalException(String message) {
        super (message);
    }
    
    public AkibanInternalException(String message, Throwable cause)  {
        super (message, cause);
    }

    public ErrorCode getCode() { 
        return ErrorCode.INTERNAL_ERROR;
    }
}
