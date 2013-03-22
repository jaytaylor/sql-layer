
package com.akiban.server.error;

public class NetworkStartIOException extends InvalidOperationException {
    public NetworkStartIOException (String message) {
        super (ErrorCode.NET_START_IO_ERROR, message);
    }
}
