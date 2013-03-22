
package com.akiban.ais.model.validation;

import com.akiban.server.error.ErrorCode;
import com.akiban.server.error.InvalidOperationException;

public class AISValidationFailure {
    public AISValidationFailure (InvalidOperationException ex) {
        this.exception = ex; 
    }

    public ErrorCode errorCode() {
        return exception.getCode();
    }
    public String message() {
        return exception.getShortMessage();
    }
    
    public void generateException() {
        throw exception;
    }
    
    private InvalidOperationException exception;

}

