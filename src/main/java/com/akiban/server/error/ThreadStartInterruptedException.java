
package com.akiban.server.error;

public class ThreadStartInterruptedException extends InvalidOperationException {
    public ThreadStartInterruptedException (String service, String message) {
        super (ErrorCode.THREAD_START_INTR, service, message);
    }
}
