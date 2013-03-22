
package com.akiban.server.error;

public class ThreadStopInterruptedException extends InvalidOperationException {
    public ThreadStopInterruptedException (String service, String message) {
        super (ErrorCode.THREAD_STOP_INTR, service, message);
    }
}
