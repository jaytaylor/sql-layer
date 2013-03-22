
package com.akiban.http;

public class HttpConductorException extends RuntimeException {
    public HttpConductorException(String message) {
        super(message);
    }

    public HttpConductorException(Throwable cause) {
        super(cause);
    }
}
