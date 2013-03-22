
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
