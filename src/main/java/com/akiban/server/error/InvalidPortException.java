
package com.akiban.server.error;

public class InvalidPortException extends InvalidOperationException {
    public InvalidPortException (int port) {
        super (ErrorCode.INVALID_PORT, port);
    }
}
