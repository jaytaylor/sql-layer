

package com.akiban.server.collation;

@SuppressWarnings("serial")
public class InvalidCollationException extends RuntimeException {

    private final String name;
    
    public InvalidCollationException(final String name) {
        this.name = name;
    }
    
    @Override
    public String getMessage() {
        return name;
    }
}
