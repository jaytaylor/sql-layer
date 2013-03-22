
package com.akiban.server.error;

public class ConnectionTerminatedException extends InvalidOperationException {

    public ConnectionTerminatedException(String reason) {
        super (ErrorCode.CONNECTION_TERMINATED, reason);
    }
}
