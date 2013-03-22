
package com.akiban.server.types;

public class ValueSourceException extends RuntimeException {
    ValueSourceException() {
    }
    
    public ValueSourceException(String message) {
        super(message);
    }

    public ValueSourceException(String message, Throwable cause) {
        super(message, cause);
    }
}
