
package com.akiban.server.error;

public class NetworkStopIOException extends InvalidOperationException {
    public NetworkStopIOException (String message) {
        super (ErrorCode.NET_STOP_IO_ERROR, message);
    }
}
