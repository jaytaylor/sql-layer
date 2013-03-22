
package com.akiban.server.error;

import static junit.framework.Assert.fail;
import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.akiban.server.error.ErrorCode;

public final class ErrorCodeTest {

    @Test
    public void errorCodesAllUnique() {
        final Map<String,ErrorCode> map = new HashMap<>(ErrorCode.values().length);
        for (ErrorCode errorCode : ErrorCode.values()) {
            ErrorCode oldCode = map.put(errorCode.getFormattedValue(), errorCode);
            if (oldCode != null) {
                fail(String.format("Conflict between codes %s and %s; both equal %s",
                        oldCode, errorCode, errorCode.getFormattedValue()));
            }
        }
    }
    
    @Test
    public void errorExceptionsUnique() {
        final Map<Class<? extends InvalidOperationException>, ErrorCode> map = new HashMap<>(ErrorCode.values().length);
        
        for (ErrorCode errorCode : ErrorCode.values()) {
            // don't check the null ones. 
            if (errorCode.associatedExceptionClass() == null) { continue; } 
            ErrorCode oldCode = map.put (errorCode.associatedExceptionClass(), errorCode);
            if (oldCode != null) {
               fail (String.format("Duplicate Exception between %s and %s, both with %s exception" , 
                       oldCode, errorCode, errorCode.associatedExceptionClass()));
           }
        }
    }

    @Test
    public void errorHasMessage() {
         for (ErrorCode errorCode : ErrorCode.values()) {
             assertNotNull (errorCode.getMessage());
         }
    }
}
